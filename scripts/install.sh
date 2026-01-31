#!/bin/bash
#
# TigerFS Install Script
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/timescale/tigerfs/main/scripts/install.sh | bash
#
# Options (via environment variables):
#   TIGERFS_INSTALL_DIR  - Installation directory (default: ~/.local/bin)
#   TIGERFS_VERSION      - Specific version to install (default: latest)
#

set -euo pipefail

# Configuration
GITHUB_REPO="timescale/tigerfs"
BINARY_NAME="tigerfs"
INSTALL_DIR="${TIGERFS_INSTALL_DIR:-$HOME/.local/bin}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

info() {
    echo -e "${BLUE}==>${NC} $1"
}

success() {
    echo -e "${GREEN}==>${NC} $1"
}

warn() {
    echo -e "${YELLOW}Warning:${NC} $1"
}

error() {
    echo -e "${RED}Error:${NC} $1" >&2
    exit 1
}

# Detect OS
detect_os() {
    local os
    os="$(uname -s)"
    case "$os" in
        Linux*)  echo "Linux" ;;
        Darwin*) echo "Darwin" ;;
        *)       error "Unsupported operating system: $os" ;;
    esac
}

# Detect architecture
detect_arch() {
    local arch
    arch="$(uname -m)"
    case "$arch" in
        x86_64|amd64)  echo "x86_64" ;;
        arm64|aarch64) echo "arm64" ;;
        *)             error "Unsupported architecture: $arch" ;;
    esac
}

# Get latest version from GitHub
get_latest_version() {
    local url="https://api.github.com/repos/${GITHUB_REPO}/releases/latest"
    local version

    if command -v curl &> /dev/null; then
        version=$(curl -fsSL "$url" 2>/dev/null | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')
    elif command -v wget &> /dev/null; then
        version=$(wget -qO- "$url" 2>/dev/null | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')
    else
        error "Neither curl nor wget found. Please install one of them."
    fi

    if [ -z "$version" ]; then
        error "Failed to fetch latest version. Check your internet connection or specify TIGERFS_VERSION."
    fi

    echo "$version"
}

# Download file with retry
download() {
    local url="$1"
    local output="$2"
    local retries=3
    local retry_delay=2

    for ((i=1; i<=retries; i++)); do
        if command -v curl &> /dev/null; then
            if curl -fsSL "$url" -o "$output" 2>/dev/null; then
                return 0
            fi
        elif command -v wget &> /dev/null; then
            if wget -q "$url" -O "$output" 2>/dev/null; then
                return 0
            fi
        fi

        if [ $i -lt $retries ]; then
            warn "Download failed, retrying in ${retry_delay}s... (attempt $i/$retries)"
            sleep $retry_delay
        fi
    done

    return 1
}

# Verify checksum
verify_checksum() {
    local file="$1"
    local checksum_file="$2"
    local expected_checksum
    local actual_checksum

    # Extract expected checksum for our file
    expected_checksum=$(grep "$(basename "$file")" "$checksum_file" | awk '{print $1}')

    if [ -z "$expected_checksum" ]; then
        error "Could not find checksum for $(basename "$file")"
    fi

    # Calculate actual checksum
    if command -v sha256sum &> /dev/null; then
        actual_checksum=$(sha256sum "$file" | awk '{print $1}')
    elif command -v shasum &> /dev/null; then
        actual_checksum=$(shasum -a 256 "$file" | awk '{print $1}')
    else
        warn "Neither sha256sum nor shasum found. Skipping checksum verification."
        return 0
    fi

    if [ "$expected_checksum" != "$actual_checksum" ]; then
        error "Checksum verification failed!\nExpected: $expected_checksum\nActual:   $actual_checksum"
    fi

    return 0
}

# Check if directory is in PATH
check_path() {
    local dir="$1"
    if [[ ":$PATH:" != *":$dir:"* ]]; then
        echo ""
        warn "$dir is not in your PATH"
        echo ""
        echo "Add it to your shell configuration:"
        echo ""
        echo "  # For bash (~/.bashrc or ~/.bash_profile):"
        echo "  export PATH=\"$dir:\$PATH\""
        echo ""
        echo "  # For zsh (~/.zshrc):"
        echo "  export PATH=\"$dir:\$PATH\""
        echo ""
        echo "Then restart your shell or run: source ~/.bashrc (or ~/.zshrc)"
        echo ""
    fi
}

# Main installation
main() {
    local os arch version archive_name download_url checksum_url
    local tmp_dir archive_file checksum_file

    info "Installing TigerFS..."

    # Detect platform
    os=$(detect_os)
    arch=$(detect_arch)
    info "Detected platform: $os $arch"

    # Get version
    if [ -n "${TIGERFS_VERSION:-}" ]; then
        version="$TIGERFS_VERSION"
        info "Using specified version: $version"
    else
        info "Fetching latest version..."
        version=$(get_latest_version)
        info "Latest version: $version"
    fi

    # Construct download URLs
    # Archive name format: tigerfs_Darwin_arm64.tar.gz or tigerfs_Linux_x86_64.tar.gz
    archive_name="${BINARY_NAME}_${os}_${arch}.tar.gz"
    download_url="https://github.com/${GITHUB_REPO}/releases/download/${version}/${archive_name}"
    checksum_url="https://github.com/${GITHUB_REPO}/releases/download/${version}/checksums.txt"

    # Create temp directory
    tmp_dir=$(mktemp -d)
    trap "rm -rf '$tmp_dir'" EXIT

    archive_file="$tmp_dir/$archive_name"
    checksum_file="$tmp_dir/checksums.txt"

    # Download archive
    info "Downloading $archive_name..."
    if ! download "$download_url" "$archive_file"; then
        error "Failed to download $download_url"
    fi

    # Download checksums
    info "Downloading checksums..."
    if ! download "$checksum_url" "$checksum_file"; then
        error "Failed to download checksums"
    fi

    # Verify checksum
    info "Verifying checksum..."
    verify_checksum "$archive_file" "$checksum_file"
    success "Checksum verified"

    # Extract archive
    info "Extracting archive..."
    tar -xzf "$archive_file" -C "$tmp_dir"

    # Create install directory if needed
    if [ ! -d "$INSTALL_DIR" ]; then
        info "Creating $INSTALL_DIR..."
        mkdir -p "$INSTALL_DIR"
    fi

    # Install binary
    info "Installing to $INSTALL_DIR/$BINARY_NAME..."
    mv "$tmp_dir/$BINARY_NAME" "$INSTALL_DIR/$BINARY_NAME"
    chmod +x "$INSTALL_DIR/$BINARY_NAME"

    success "TigerFS $version installed successfully!"
    echo ""

    # Verify installation
    if [ -x "$INSTALL_DIR/$BINARY_NAME" ]; then
        echo "Installed: $INSTALL_DIR/$BINARY_NAME"
        "$INSTALL_DIR/$BINARY_NAME" version 2>/dev/null || true
    fi

    # Check PATH
    check_path "$INSTALL_DIR"
}

main "$@"
