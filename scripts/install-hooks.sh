#!/bin/sh
#
# Install git hooks for local development

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
HOOKS_DIR="$REPO_ROOT/.git/hooks"

echo "Installing git hooks..."

# Install pre-commit hook
ln -sf "$SCRIPT_DIR/pre-commit" "$HOOKS_DIR/pre-commit"
echo "Installed pre-commit hook"

echo "Done! Hooks installed successfully."
