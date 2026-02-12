#!/bin/bash
#
# TigerFS DDL Example: Modify Table
#
# This script demonstrates the workflow for modifying an existing table
# using TigerFS's DDL staging pattern. It adds a new column to a table.
#
# Prerequisites:
#   - TigerFS mounted at $MOUNT_POINT (default: /mnt/db)
#   - The target table must exist
#   - Write access to the database
#
# Usage:
#   ./modify-table.sh [mount_point] [table_name]
#

set -e

MOUNT_POINT="${1:-/mnt/db}"
TABLE_NAME="${2:-users}"

echo "=== TigerFS DDL Example: Modify Table ==="
echo "Mount point: $MOUNT_POINT"
echo "Table name:  $TABLE_NAME"
echo

# Verify table exists
if [ ! -d "$MOUNT_POINT/$TABLE_NAME" ]; then
    echo "Error: Table '$TABLE_NAME' does not exist at $MOUNT_POINT/$TABLE_NAME"
    echo "Please specify an existing table or create one first."
    exit 1
fi

# Step 1: View current schema via template
echo "Step 1: Viewing current schema..."
echo "  Current table structure (from template):"
cat "$MOUNT_POINT/$TABLE_NAME/.modify/sql" | head -20 | sed 's/^/    /'
echo "    ..."
echo

# Step 2: View current columns
echo "Step 2: Current columns:"
cat "$MOUNT_POINT/$TABLE_NAME/.info/columns" | sed 's/^/    /'
echo

# Step 3: Write ALTER TABLE statement
COLUMN_NAME="last_modified_at"
echo "Step 3: Adding new column '$COLUMN_NAME'..."
cat > "$MOUNT_POINT/$TABLE_NAME/.modify/sql" << EOF
ALTER TABLE $TABLE_NAME ADD COLUMN $COLUMN_NAME TIMESTAMP WITH TIME ZONE DEFAULT NOW();
EOF
echo "  DDL staged successfully"
echo

# Step 4: Validate the DDL
echo "Step 4: Validating DDL..."
touch "$MOUNT_POINT/$TABLE_NAME/.modify/.test"
echo "  Validation result:"
cat "$MOUNT_POINT/$TABLE_NAME/.modify/test.log" | sed 's/^/    /'
echo

# Check for validation errors
if ! grep -q "OK" "$MOUNT_POINT/$TABLE_NAME/.modify/test.log" 2>/dev/null; then
    echo "  Warning: Validation may have failed. Check test.log for details."
    echo "  Aborting modification..."
    touch "$MOUNT_POINT/$TABLE_NAME/.modify/.abort"
    exit 1
fi

# Step 5: Commit the DDL
echo "Step 5: Committing DDL..."
touch "$MOUNT_POINT/$TABLE_NAME/.modify/.commit"
echo "  Table modified successfully!"
echo

# Step 6: Verify the modification
echo "Step 6: Verifying modification..."
echo "  Updated columns:"
cat "$MOUNT_POINT/$TABLE_NAME/.info/columns" | sed 's/^/    /'
echo
echo "  Column details:"
cat "$MOUNT_POINT/$TABLE_NAME/.info/schema" | grep -E "(^|$COLUMN_NAME)" | sed 's/^/    /'
echo

echo "=== Modify Table Example Complete ==="
echo
echo "The column '$COLUMN_NAME' has been added to table '$TABLE_NAME'."
