#!/bin/sh
#
# Integration tests for scripts/install.sh
#
# Verifies the full install flow end-to-end: download, checksum verification,
# binary extraction, and skills installation across different coding agents.
#
# How it works:
#   - Builds a real tar.gz archive with a stub binary and skill files
#   - Creates a mock server directory with the archive + sha256 checksum
#   - Points install.sh at the mock server via BASE_URL=file:// (curl and
#     wget both handle file:// URLs natively, so no HTTP server needed)
#   - Overrides HOME and INSTALL_DIR to temp dirs, so nothing on the real
#     system is touched
#
# This tests the actual install.sh code path -- no functions are mocked or
# overridden. The only difference from a real install is the transport
# (file:// instead of https://).
#
# Usage:
#   ./scripts/test-install.sh
#
# Requirements:
#   - sh, tar, curl or wget, sha256sum or shasum (same as install.sh itself)
#
set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INSTALL_SH="$SCRIPT_DIR/install.sh"

PASS=0
FAIL=0

# --- Test helpers ---

setup() {
  TEST_DIR="$(mktemp -d)"
  MOCK_SERVER="$TEST_DIR/server"
  MOCK_HOME="$TEST_DIR/home"
  MOCK_BIN="$TEST_DIR/bin"

  mkdir -p "$MOCK_HOME" "$MOCK_BIN"

  # Detect OS/arch to match what install.sh expects
  case "$(uname -s)" in
    Linux*)  TEST_OS="Linux" ;;
    Darwin*) TEST_OS="Darwin" ;;
    *)       echo "Unsupported OS"; exit 1 ;;
  esac
  case "$(uname -m)" in
    x86_64|amd64)  TEST_ARCH="x86_64" ;;
    arm64|aarch64) TEST_ARCH="arm64" ;;
    *)             echo "Unsupported arch"; exit 1 ;;
  esac

  # Build mock archive containing a stub binary and skill files
  archive_name="tigerfs_${TEST_OS}_${TEST_ARCH}.tar.gz"
  staging="$TEST_DIR/staging"
  mkdir -p "$staging/skills/tigerfs"

  # Stub binary that prints a version string (enough for install.sh's
  # "$dest" version call to succeed)
  printf '#!/bin/sh\necho "tigerfs v0.0.0-test"\n' > "$staging/tigerfs"
  chmod +x "$staging/tigerfs"

  # Skill files matching what the real archive ships
  printf -- '---\nname: tigerfs\ndescription: test skill\n---\n# Test\n' > "$staging/skills/tigerfs/SKILL.md"
  printf 'apps content\n'       > "$staging/skills/tigerfs/apps.md"
  printf 'native-ops content\n' > "$staging/skills/tigerfs/native-ops.md"
  printf 'history content\n'    > "$staging/skills/tigerfs/history.md"
  printf 'recipes content\n'    > "$staging/skills/tigerfs/recipes.md"

  # Create the archive
  tar -czf "$TEST_DIR/$archive_name" -C "$staging" tigerfs skills

  # Lay out the mock server directory to mirror the real CDN structure:
  #   $MOCK_SERVER/latest.txt
  #   $MOCK_SERVER/releases/v0.0.0-test/tigerfs_Darwin_arm64.tar.gz
  #   $MOCK_SERVER/releases/v0.0.0-test/tigerfs_Darwin_arm64.tar.gz.sha256
  release_dir="$MOCK_SERVER/releases/v0.0.0-test"
  mkdir -p "$release_dir"
  cp "$TEST_DIR/$archive_name" "$release_dir/"

  # Generate a real checksum so install.sh's verification passes
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$release_dir/$archive_name" | awk '{print $1}' > "$release_dir/${archive_name}.sha256"
  else
    shasum -a 256 "$release_dir/$archive_name" | awk '{print $1}' > "$release_dir/${archive_name}.sha256"
  fi

  printf 'v0.0.0-test\n' > "$MOCK_SERVER/latest.txt"
}

cleanup() {
  rm -rf "$TEST_DIR"
}

run_install() {
  # Run install.sh against the mock server with an isolated HOME.
  # $1: path to file for stdin (defaults to /dev/null for non-interactive)
  # $2: if "interactive", sets TIGERFS_INTERACTIVE=1 to force the agent prompt
  stdin_source="${1:-/dev/null}"
  interactive="${2:-}"
  if [ "$interactive" = "interactive" ]; then
    export TIGERFS_INTERACTIVE=1
  else
    unset TIGERFS_INTERACTIVE 2>/dev/null || true
  fi
  HOME="$MOCK_HOME" \
  INSTALL_DIR="$MOCK_BIN" \
  BASE_URL="file://$MOCK_SERVER" \
  VERSION="v0.0.0-test" \
    sh "$INSTALL_SH" < "$stdin_source" >/dev/null 2>&1
}

assert_file_exists() {
  if [ -f "$1" ]; then
    return 0
  else
    echo "    MISSING: $1"
    return 1
  fi
}

assert_dir_exists() {
  if [ -d "$1" ]; then
    return 0
  else
    echo "    MISSING DIR: $1"
    return 1
  fi
}

assert_file_not_exists() {
  if [ ! -f "$1" ]; then
    return 0
  else
    echo "    SHOULD NOT EXIST: $1"
    return 1
  fi
}

assert_skill_files() {
  dir="$1"
  assert_file_exists "$dir/SKILL.md" &&
  assert_file_exists "$dir/apps.md" &&
  assert_file_exists "$dir/native-ops.md" &&
  assert_file_exists "$dir/history.md" &&
  assert_file_exists "$dir/recipes.md"
}

run_test() {
  name="$1"
  printf "%-50s " "$name"
  if eval "$2"; then
    printf "PASS\n"
    PASS=$((PASS + 1))
  else
    printf "FAIL\n"
    FAIL=$((FAIL + 1))
  fi
}

# --- Tests ---

test_noninteractive_install() {
  setup
  run_install /dev/null

  # Binary installed
  assert_file_exists "$MOCK_BIN/tigerfs" &&

  # Skills staged to fallback location (non-interactive skips agent prompt)
  assert_skill_files "$MOCK_HOME/.config/tigerfs/skills/tigerfs"

  result=$?
  cleanup
  return $result
}

test_interactive_select_claude() {
  setup

  # Create .claude dir so Claude Code is detected
  mkdir -p "$MOCK_HOME/.claude"

  # Pipe "1" to select first detected agent (Claude Code)
  printf '1\n' > "$TEST_DIR/input"
  run_install "$TEST_DIR/input" interactive

  assert_skill_files "$MOCK_HOME/.claude/skills/tigerfs"

  result=$?
  cleanup
  return $result
}

test_interactive_select_all() {
  setup

  # Create dirs for multiple agents
  mkdir -p "$MOCK_HOME/.claude"
  mkdir -p "$MOCK_HOME/.cursor"
  mkdir -p "$MOCK_HOME/.kiro"

  # Pipe "a" to select all detected
  printf 'a\n' > "$TEST_DIR/input"
  run_install "$TEST_DIR/input" interactive

  assert_skill_files "$MOCK_HOME/.claude/skills/tigerfs" &&
  assert_skill_files "$MOCK_HOME/.cursor/skills/tigerfs" &&
  assert_skill_files "$MOCK_HOME/.kiro/steering/tigerfs" &&

  # Agents NOT detected should NOT have skills installed
  assert_file_not_exists "$MOCK_HOME/.agents/skills/tigerfs/SKILL.md" &&
  assert_file_not_exists "$MOCK_HOME/.codeium/windsurf/skills/tigerfs/SKILL.md"

  result=$?
  cleanup
  return $result
}

test_interactive_skip() {
  setup
  mkdir -p "$MOCK_HOME/.claude"

  # Pipe "s" to skip
  printf 's\n' > "$TEST_DIR/input"
  run_install "$TEST_DIR/input" interactive

  # Skills should be staged, not installed to agent dir
  assert_skill_files "$MOCK_HOME/.config/tigerfs/skills/tigerfs" &&
  assert_file_not_exists "$MOCK_HOME/.claude/skills/tigerfs/SKILL.md"

  result=$?
  cleanup
  return $result
}

test_upgrade_removes_stale() {
  setup
  mkdir -p "$MOCK_HOME/.claude"

  # First install
  printf '1\n' > "$TEST_DIR/input"
  run_install "$TEST_DIR/input" interactive

  # Add a stale file (simulates a skill file removed in a newer version)
  printf 'stale\n' > "$MOCK_HOME/.claude/skills/tigerfs/old-skill.md"
  assert_file_exists "$MOCK_HOME/.claude/skills/tigerfs/old-skill.md" || return 1

  # Re-install (upgrade)
  run_install "$TEST_DIR/input" interactive

  # Stale file should be gone, current files present
  assert_file_not_exists "$MOCK_HOME/.claude/skills/tigerfs/old-skill.md" &&
  assert_skill_files "$MOCK_HOME/.claude/skills/tigerfs"

  result=$?
  cleanup
  return $result
}

test_no_agents_detected() {
  setup

  # Don't create any agent dirs. With no agents detected, the interactive
  # prompt falls back to staging regardless of input.
  printf '1\n' > "$TEST_DIR/input"
  run_install "$TEST_DIR/input" interactive

  assert_skill_files "$MOCK_HOME/.config/tigerfs/skills/tigerfs"

  result=$?
  cleanup
  return $result
}

test_second_agent_selection() {
  setup

  # Create dirs for two agents
  mkdir -p "$MOCK_HOME/.claude"
  mkdir -p "$MOCK_HOME/.cursor"

  # Select "2" which maps to Cursor (second detected agent)
  printf '2\n' > "$TEST_DIR/input"
  run_install "$TEST_DIR/input" interactive

  # Cursor should have skills, Claude should not
  assert_skill_files "$MOCK_HOME/.cursor/skills/tigerfs" &&
  assert_file_not_exists "$MOCK_HOME/.claude/skills/tigerfs/SKILL.md"

  result=$?
  cleanup
  return $result
}

test_select_all_single_agent() {
  setup

  # Only one agent detected
  mkdir -p "$MOCK_HOME/.claude"

  # Pipe "a" to select all (should install only to Claude)
  printf 'a\n' > "$TEST_DIR/input"
  run_install "$TEST_DIR/input" interactive

  assert_skill_files "$MOCK_HOME/.claude/skills/tigerfs" &&

  # No other agents should have skills
  assert_file_not_exists "$MOCK_HOME/.cursor/skills/tigerfs/SKILL.md" &&
  assert_file_not_exists "$MOCK_HOME/.agents/skills/tigerfs/SKILL.md"

  result=$?
  cleanup
  return $result
}

# --- Run tests ---

printf "\n=== TigerFS Install Script Tests ===\n\n"

run_test "Non-interactive install (piped stdin)" test_noninteractive_install
run_test "Interactive: select Claude Code"       test_interactive_select_claude
run_test "Interactive: select all detected"      test_interactive_select_all
run_test "Interactive: all with single agent"    test_select_all_single_agent
run_test "Interactive: skip"                     test_interactive_skip
run_test "Upgrade: removes stale files"          test_upgrade_removes_stale
run_test "No agents detected: falls back"        test_no_agents_detected
run_test "Interactive: select second agent"      test_second_agent_selection

printf "\n=== Results: %d passed, %d failed ===\n\n" "$PASS" "$FAIL"

if [ "$FAIL" -gt 0 ]; then
  exit 1
fi
