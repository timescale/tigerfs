#!/bin/bash
#
# TigerFS DDL Example: Create Table
#
# This script demonstrates the complete workflow for creating a new table
# using TigerFS's DDL staging pattern.
#
# Prerequisites:
#   - TigerFS mounted at $MOUNT_POINT (default: /mnt/db)
#   - Write access to the database
#
# Usage:
#   ./create-table.sh [mount_point]
#

set -e

MOUNT_POINT="${1:-/mnt/db}"
TABLE_NAME="example_orders"

echo "=== TigerFS DDL Example: Create Table ==="
echo "Mount point: $MOUNT_POINT"
echo "Table name:  $TABLE_NAME"
echo

# Step 1: Create staging directory
echo "Step 1: Creating staging directory..."
mkdir -p "$MOUNT_POINT/.create/$TABLE_NAME"
echo "  Created: $MOUNT_POINT/.create/$TABLE_NAME/"
echo

# Step 2: View the template (optional, for human workflows)
echo "Step 2: Viewing template..."
echo "  Template contents:"
cat "$MOUNT_POINT/.create/$TABLE_NAME/sql" | sed 's/^/    /'
echo

# Step 3: Write the DDL statement
echo "Step 3: Writing DDL statement..."
cat > "$MOUNT_POINT/.create/$TABLE_NAME/sql" << 'EOF'
CREATE TABLE example_orders (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    product_name TEXT NOT NULL,
    quantity INTEGER DEFAULT 1,
    total_price NUMERIC(10,2) NOT NULL,
    status TEXT DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
EOF
echo "  DDL staged successfully"
echo

# Step 4: Validate the DDL (optional but recommended)
echo "Step 4: Validating DDL..."
touch "$MOUNT_POINT/.create/$TABLE_NAME/.test"
echo "  Validation result:"
cat "$MOUNT_POINT/.create/$TABLE_NAME/test.log" | sed 's/^/    /'
echo

# Step 5: Commit the DDL
echo "Step 5: Committing DDL..."
touch "$MOUNT_POINT/.create/$TABLE_NAME/.commit"
echo "  Table created successfully!"
echo

# Step 6: Verify the table exists
echo "Step 6: Verifying table creation..."
echo "  Table schema:"
cat "$MOUNT_POINT/$TABLE_NAME/.info/schema" | sed 's/^/    /'
echo
echo "  Listing table directory:"
ls -la "$MOUNT_POINT/$TABLE_NAME/" | sed 's/^/    /'
echo

echo "=== Create Table Example Complete ==="
echo
echo "To clean up, run:"
echo "  echo 'DROP TABLE $TABLE_NAME' > $MOUNT_POINT/$TABLE_NAME/.delete/sql"
echo "  touch $MOUNT_POINT/$TABLE_NAME/.delete/.commit"
