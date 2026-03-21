#!/bin/sh
set -eu

BASE_URL="${BASE_URL:-https://install.tigerfs.io}"
BINARY="tigerfs"
MAX_RETRIES=3

# --- Colors (disabled if not a terminal) ---

if [ -t 1 ]; then
  BOLD='\033[1m'
  DIM='\033[2m'
  GREEN='\033[32m'
  RED='\033[31m'
  YELLOW='\033[33m'
  CYAN='\033[36m'
  RESET='\033[0m'
else
  BOLD='' DIM='' GREEN='' RED='' YELLOW='' CYAN='' RESET=''
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

  # Install skills
  skills_src="$tmp_dir/skills/tigerfs"
  if [ -d "$skills_src" ]; then
    install_skills "$skills_src"
  fi

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

# --- Skills installation ---

# Agent name:skills path (relative to $HOME):detection dir (relative to $HOME)
AGENT_COUNT=7
AGENT_1_NAME="Claude Code"    AGENT_1_PATH=".claude/skills"              AGENT_1_DETECT=".claude"
AGENT_2_NAME="Cursor"         AGENT_2_PATH=".cursor/skills"              AGENT_2_DETECT=".cursor"
AGENT_3_NAME="Codex CLI"      AGENT_3_PATH=".agents/skills"              AGENT_3_DETECT=".codex"
AGENT_4_NAME="Gemini CLI"     AGENT_4_PATH=".gemini/skills"              AGENT_4_DETECT=".gemini"
AGENT_5_NAME="Windsurf"       AGENT_5_PATH=".codeium/windsurf/skills"    AGENT_5_DETECT=".codeium/windsurf"
AGENT_6_NAME="Antigravity"    AGENT_6_PATH=".gemini/antigravity/skills"  AGENT_6_DETECT=".gemini/antigravity"
AGENT_7_NAME="Kiro"           AGENT_7_PATH=".kiro/steering"              AGENT_7_DETECT=".kiro"

# Helper to get agent field by index
agent_name()   { eval echo "\$AGENT_${1}_NAME"; }
agent_path()   { eval echo "\$AGENT_${1}_PATH"; }
agent_detect() { eval echo "\$AGENT_${1}_DETECT"; }

is_agent_detected() {
  detect_dir="$(agent_detect "$1")"
  [ -d "$HOME/$detect_dir" ]
}

install_skills() {
  skills_src="$1"

  # Prompt if a human is watching (stdout is a terminal) or forced interactive.
  # This works even with `curl | sh` because we read from /dev/tty, not stdin.
  if [ -t 1 ] || [ "${TIGERFS_INTERACTIVE:-}" = "1" ]; then
    interactive_install_skills "$skills_src"
  else
    stage_skills "$skills_src"
  fi
}

interactive_install_skills() {
  skills_src="$1"

  # Build list of detected agents and display menu
  detected=""
  num=1
  printf "\n"
  info "Install TigerFS skills for your coding agent(s):"

  i=1
  while [ "$i" -le "$AGENT_COUNT" ]; do
    name="$(agent_name "$i")"
    path="$(agent_path "$i")"
    if is_agent_detected "$i"; then
      printf "   ${BOLD}%d)${RESET} %-16s (~/%s/)\n" "$num" "$name" "$path"
      detected="${detected}${i}:${num} "
      num=$((num + 1))
    else
      printf "   ${DIM}-  %-16s (not detected)${RESET}\n" "$name"
    fi
    i=$((i + 1))
  done

  max_choice=$((num - 1))

  if [ "$max_choice" -eq 0 ]; then
    info "No coding agents detected. Staging skills for later."
    stage_skills "$skills_src"
    return
  fi

  printf "   ${BOLD}a)${RESET} All detected\n"
  printf "   ${BOLD}s)${RESET} Skip\n"
  printf "\n"

  if [ "$max_choice" -eq 1 ]; then
    printf "Choice [1, a, s] (default: a): "
  else
    printf "Choice [1-%d, a, s] (default: a): " "$max_choice"
  fi
  # Read from /dev/tty (works with `curl | sh`), fall back to stdin for tests
  if [ -e /dev/tty ] && [ "${TIGERFS_INTERACTIVE:-}" != "1" ]; then
    read -r choice </dev/tty || choice="s"
  else
    read -r choice || choice="s"
  fi
  [ -z "$choice" ] && choice="a"

  case "$choice" in
    s|S)
      stage_skills "$skills_src"
      ;;
    a|A)
      for pair in $detected; do
        agent_idx="${pair%%:*}"
        copy_skills_to_agent "$skills_src" "$(agent_name "$agent_idx")" "$HOME/$(agent_path "$agent_idx")"
      done
      ;;
    *)
      # Map display number back to agent index
      found=""
      for pair in $detected; do
        agent_idx="${pair%%:*}"
        display_num="${pair##*:}"
        if [ "$display_num" = "$choice" ]; then
          found="$agent_idx"
          break
        fi
      done
      if [ -n "$found" ]; then
        copy_skills_to_agent "$skills_src" "$(agent_name "$found")" "$HOME/$(agent_path "$found")"
      else
        warn "Invalid choice, skipping skills installation"
        stage_skills "$skills_src"
      fi
      ;;
  esac
}

copy_skills_to_agent() {
  src="$1"
  agent_name="$2"
  agent_skills_dir="$3"

  rm -rf "$agent_skills_dir/tigerfs"
  mkdir -p "$agent_skills_dir"
  cp -r "$src" "$agent_skills_dir/tigerfs"
  success "Skills installed for ${BOLD}${agent_name}${RESET} at ${agent_skills_dir}/tigerfs/"
}

stage_skills() {
  src="$1"
  stage_dir="$HOME/.config/tigerfs/skills"

  rm -rf "$stage_dir/tigerfs"
  mkdir -p "$stage_dir"
  cp -r "$src" "$stage_dir/tigerfs"
  info "Skills staged to ${BOLD}${stage_dir}/tigerfs/${RESET}"

  # Detect installed agents and print specific copy commands
  found_any=""
  i=1
  while [ "$i" -le "$AGENT_COUNT" ]; do
    if is_agent_detected "$i"; then
      if [ -z "$found_any" ]; then
        printf "   Install for your detected agent(s):\n"
        found_any="1"
      fi
      printf "     cp -r %s/tigerfs ~/%s/tigerfs\n" "$stage_dir" "$(agent_path "$i")"
    fi
    i=$((i + 1))
  done

  if [ -z "$found_any" ]; then
    printf "   Copy to your agent's skills directory, e.g.:\n"
    printf "     cp -r %s/tigerfs ~/.claude/skills/\n" "$stage_dir"
  fi
  printf "\n"
}

# --- Output ---

info()    { printf "${CYAN}=>${RESET} %b\n" "$*"; }
success() { printf "${GREEN}=>${RESET} %b\n" "$*"; }
warn()    { printf "${YELLOW}=>${RESET} %b\n" "$*" >&2; }
err()     { printf "${RED}error:${RESET} %b\n" "$*" >&2; exit 1; }

main
