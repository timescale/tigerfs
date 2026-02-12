#!/bin/bash
#
# TigerFS DDL Example: Delete Table
#
# This script demonstrates the workflow for deleting a table using TigerFS's
# DDL staging pattern. For safety, it creates a temporary table first,
# then deletes it.
#
# Prerequisites:
#   - TigerFS mounted at $MOUNT_POINT (default: /mnt/db)
#   - Write access to the database
#
# Usage:
#   ./delete-table.sh [mount_point]
#

set -e

MOUNT_POINT="${1:-/mnt/db}"
TABLE_NAME="temp_delete_example"

echo "=== TigerFS DDL Example: Delete Table ==="
echo "Mount point: $MOUNT_POINT"
echo "Table name:  $TABLE_NAME"
echo

# Step 1: Create a temporary table to delete
echo "Step 1: Creating temporary table for demonstration..."
mkdir -p "$MOUNT_POINT/.create/$TABLE_NAME"
cat > "$MOUNT_POINT/.create/$TABLE_NAME/sql" << 'EOF'
CREATE TABLE temp_delete_example (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
EOF
touch "$MOUNT_POINT/.create/$TABLE_NAME/.commit"
echo "  Temporary table created"
echo

# Step 2: Add some sample data
echo "Step 2: Adding sample data..."
mkdir "$MOUNT_POINT/$TABLE_NAME/1" 2>/dev/null || true
echo "Test Row 1" > "$MOUNT_POINT/$TABLE_NAME/1/name.txt"
mkdir "$MOUNT_POINT/$TABLE_NAME/2" 2>/dev/null || true
echo "Test Row 2" > "$MOUNT_POINT/$TABLE_NAME/2/name.txt"
echo "  Added 2 sample rows"
echo

# Step 3: View table contents before deletion
echo "Step 3: Table contents before deletion:"
ls -la "$MOUNT_POINT/$TABLE_NAME/" | head -10 | sed 's/^/    /'
echo
echo "  Row count:"
cat "$MOUNT_POINT/$TABLE_NAME/.info/count" | sed 's/^/    /'
echo

# Step 4: View the delete template (shows impact)
echo "Step 4: Viewing delete template (shows row count and dependencies)..."
echo "  Template contents:"
cat "$MOUNT_POINT/$TABLE_NAME/.delete/sql" | sed 's/^/    /'
echo

# Step 5: Write DROP TABLE statement
echo "Step 5: Writing DROP TABLE statement..."
echo "DROP TABLE $TABLE_NAME;" > "$MOUNT_POINT/$TABLE_NAME/.delete/sql"
echo "  DDL staged successfully"
echo

# Step 6: Validate the DDL
echo "Step 6: Validating DDL..."
touch "$MOUNT_POINT/$TABLE_NAME/.delete/.test"
echo "  Validation result:"
cat "$MOUNT_POINT/$TABLE_NAME/.delete/test.log" | sed 's/^/    /'
echo

# Step 7: Commit the deletion
echo "Step 7: Committing deletion..."
touch "$MOUNT_POINT/$TABLE_NAME/.delete/.commit"
echo "  Table deleted successfully!"
echo

# Step 8: Verify deletion
echo "Step 8: Verifying deletion..."
if [ -d "$MOUNT_POINT/$TABLE_NAME" ]; then
    echo "  Warning: Table directory still exists (may need metadata refresh)"
else
    echo "  Confirmed: Table '$TABLE_NAME' no longer exists"
fi
echo

# List tables to confirm
echo "  Current tables (should not include $TABLE_NAME):"
ls "$MOUNT_POINT/" | grep -v "^\." | head -10 | sed 's/^/    /'
echo

echo "=== Delete Table Example Complete ==="
echo
echo "The table '$TABLE_NAME' has been deleted from the database."
