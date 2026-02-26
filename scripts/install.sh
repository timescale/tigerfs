#!/bin/sh
set -eu

BASE_URL="https://install.tigerfs.io"
BINARY="tigerfs"
MAX_RETRIES=3

# --- Colors (disabled if not a terminal) ---

if [ -t 1 ]; then
  BOLD='\033[1m'
  GREEN='\033[32m'
  RED='\033[31m'
  YELLOW='\033[33m'
  CYAN='\033[36m'
  RESET='\033[0m'
else
  BOLD='' GREEN='' RED='' YELLOW='' CYAN='' RESET=''
fi

main() {
  check_dependencies

  os="$(detect_os)"
  arch="$(detect_arch)"
  archive="${BINARY}_${os}_${arch}.tar.gz"

  if [ -z "${VERSION:-}" ]; then
    version="$(fetch_latest_version)"
  else
    version="$VERSION"
    # Ensure version starts with 'v'
    case "$version" in
      v*) ;;
      *)  version="v$version" ;;
    esac
  fi

  install_dir="$(resolve_install_dir)"

  info "Installing ${BOLD}${BINARY} ${version}${RESET} (${os}/${arch})"

  mkdir -p "$install_dir"

  archive_url="${BASE_URL}/releases/${version}/${archive}"
  checksum_url="${archive_url}.sha256"

  # Download archive
  tmp_dir="$(mktemp -d)"
  trap 'rm -rf "$tmp_dir"' EXIT

  archive_file="${tmp_dir}/${archive}"
  checksum_file="${tmp_dir}/${archive}.sha256"

  info "Downloading ${CYAN}${archive_url}${RESET}"
  download_with_retry "$archive_url" "$archive_file"

  # Verify checksum
  info "Verifying checksum..."
  download_with_retry "$checksum_url" "$checksum_file"
  verify_checksum "$archive_file" "$checksum_file"

  # Extract
  tar -xzf "$archive_file" -C "$tmp_dir"

  # Install binary (rm -f first to handle replacing a running binary)
  dest="${install_dir}/${BINARY}"
  rm -f "$dest"
  cp "$tmp_dir/$BINARY" "$dest"
  chmod +x "$dest"

  # macOS: remove quarantine attribute
  if [ "$os" = "Darwin" ]; then
    xattr -d com.apple.quarantine "$dest" 2>/dev/null || true
  fi

  success "Installed to ${BOLD}${dest}${RESET}"

  if ! echo "$PATH" | tr ':' '\n' | grep -qx "$install_dir"; then
    warn "Add ${BOLD}${install_dir}${RESET} to your PATH:"
    printf "    export PATH=\"%s:\$PATH\"\n\n" "$install_dir"
  fi

  "$dest" version 2>/dev/null || true
}

# --- Detection ---

detect_os() {
  case "$(uname -s)" in
    Linux*)  echo "Linux" ;;
    Darwin*) echo "Darwin" ;;
    *)       err "Unsupported OS: $(uname -s)" ;;
  esac
}

detect_arch() {
  case "$(uname -m)" in
    x86_64|amd64)  echo "x86_64" ;;
    arm64|aarch64) echo "arm64" ;;
    *)             err "Unsupported architecture: $(uname -m)" ;;
  esac
}

check_dependencies() {
  if ! command -v curl >/dev/null 2>&1 && ! command -v wget >/dev/null 2>&1; then
    err "curl or wget is required"
  fi

  if ! command -v tar >/dev/null 2>&1; then
    err "tar is required"
  fi

  if ! command -v sha256sum >/dev/null 2>&1 && ! command -v shasum >/dev/null 2>&1; then
    err "sha256sum or shasum is required for checksum verification"
  fi
}

# --- Install directory ---

resolve_install_dir() {
  if [ -n "${INSTALL_DIR:-}" ]; then
    echo "$INSTALL_DIR"
    return
  fi

  # Prefer ~/.local/bin if it exists or parent exists
  local_bin="$HOME/.local/bin"
  if [ -d "$local_bin" ] || [ -d "$HOME/.local" ]; then
    echo "$local_bin"
    return
  fi

  # Fall back to ~/bin
  echo "$HOME/bin"
}

# --- Version ---

fetch_latest_version() {
  tmpfile="$(mktemp)"
  download_with_retry "${BASE_URL}/latest.txt" "$tmpfile"
  version="$(tr -d '[:space:]' < "$tmpfile")"
  rm -f "$tmpfile"

  if [ -z "$version" ]; then
    err "Failed to determine latest version"
  fi

  echo "$version"
}

# --- Download with retry ---

download_with_retry() {
  url="$1"
  output="$2"
  attempt=1

  while [ "$attempt" -le "$MAX_RETRIES" ]; do
    if download "$url" "$output"; then
      return 0
    fi

    if [ "$attempt" -lt "$MAX_RETRIES" ]; then
      delay=$((attempt * attempt))
      warn "Download failed (attempt ${attempt}/${MAX_RETRIES}), retrying in ${delay}s..."
      sleep "$delay"
    fi

    attempt=$((attempt + 1))
  done

  err "Download failed after ${MAX_RETRIES} attempts: ${url}"
}

download() {
  if command -v curl >/dev/null 2>&1; then
    curl -sSfL "$1" -o "$2"
  elif command -v wget >/dev/null 2>&1; then
    wget -qO "$2" "$1"
  fi
}

# --- Checksum verification ---

verify_checksum() {
  file="$1"
  checksum_file="$2"

  expected="$(awk '{print $1}' "$checksum_file")"

  if command -v sha256sum >/dev/null 2>&1; then
    actual="$(sha256sum "$file" | awk '{print $1}')"
  elif command -v shasum >/dev/null 2>&1; then
    actual="$(shasum -a 256 "$file" | awk '{print $1}')"
  else
    err "sha256sum or shasum required"
  fi

  if [ "$expected" != "$actual" ]; then
    err "Checksum mismatch!\n  Expected: ${expected}\n  Actual:   ${actual}"
  fi

  success "Checksum verified"
}

# --- Output ---

info()    { printf "${CYAN}=>${RESET} %b\n" "$*"; }
success() { printf "${GREEN}=>${RESET} %b\n" "$*"; }
warn()    { printf "${YELLOW}=>${RESET} %b\n" "$*" >&2; }
err()     { printf "${RED}error:${RESET} %b\n" "$*" >&2; exit 1; }

main
