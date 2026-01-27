// Package util provides utility functions for TigerFS.
package util

import "os"

// MapPermissions converts PostgreSQL table privileges to Unix file permissions.
//
// The mapping logic:
//   - SELECT privilege → read permission (0400)
//   - UPDATE or INSERT privilege → write permission (0200)
//   - No execute permission for data files
//
// Permission bits are combined based on what privileges are granted.
// When no privileges exist, returns 0 (no access).
//
// Parameters:
//   - canSelect: true if user has SELECT privilege
//   - canUpdate: true if user has UPDATE privilege
//   - canInsert: true if user has INSERT privilege
//   - canDelete: true if user has DELETE privilege (unused for mode bits)
//
// Returns the Unix file mode representing the effective permissions.
// Note: canDelete affects whether rm operations succeed, not the mode bits.
//
// Examples:
//   - SELECT only → 0400 (r--------)
//   - SELECT + UPDATE → 0600 (rw-------)
//   - UPDATE only (no SELECT) → 0200 (-w-------)
//   - All privileges → 0600 (rw-------)
func MapPermissions(canSelect, canUpdate, canInsert, canDelete bool) os.FileMode {
	// Note: canDelete parameter is kept for API completeness but doesn't
	// affect file mode bits. Delete permission is enforced at the operation
	// level (Unlink/Rmdir), not through mode bits.
	_ = canDelete

	var mode os.FileMode = 0

	// SELECT → read permission
	if canSelect {
		mode |= 0400
	}

	// UPDATE or INSERT → write permission
	// Both are needed because:
	// - UPDATE allows modifying existing rows/columns
	// - INSERT allows creating new rows via file writes
	if canUpdate || canInsert {
		mode |= 0200
	}

	return mode
}

// MapDirectoryPermissions converts PostgreSQL table privileges to Unix directory permissions.
//
// Directories need execute permission for traversal (cd, ls contents).
// The mapping logic:
//   - SELECT privilege → read + execute (0500)
//   - UPDATE or INSERT privilege → write (0200)
//
// Parameters:
//   - canSelect: true if user has SELECT privilege
//   - canUpdate: true if user has UPDATE privilege
//   - canInsert: true if user has INSERT privilege
//   - canDelete: true if user has DELETE privilege (unused for mode bits)
//
// Returns the Unix directory mode representing the effective permissions.
//
// Examples:
//   - SELECT only → 0500 (r-x------)
//   - SELECT + UPDATE → 0700 (rwx------)
//   - No privileges → 0000 (no access)
func MapDirectoryPermissions(canSelect, canUpdate, canInsert, canDelete bool) os.FileMode {
	_ = canDelete

	var mode os.FileMode = 0

	// SELECT → read + execute for directories
	// Execute is needed to traverse into the directory
	if canSelect {
		mode |= 0500
	}

	// UPDATE or INSERT → write permission
	if canUpdate || canInsert {
		mode |= 0200
	}

	return mode
}
