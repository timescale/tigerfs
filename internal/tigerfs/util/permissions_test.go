package util

import (
	"os"
	"testing"
)

func TestMapPermissions(t *testing.T) {
	tests := []struct {
		name      string
		canSelect bool
		canUpdate bool
		canInsert bool
		canDelete bool
		want      os.FileMode
	}{
		{
			name:      "no permissions",
			canSelect: false,
			canUpdate: false,
			canInsert: false,
			canDelete: false,
			want:      0,
		},
		{
			name:      "SELECT only (read-only)",
			canSelect: true,
			canUpdate: false,
			canInsert: false,
			canDelete: false,
			want:      0400,
		},
		{
			name:      "SELECT + UPDATE (read-write)",
			canSelect: true,
			canUpdate: true,
			canInsert: false,
			canDelete: false,
			want:      0600,
		},
		{
			name:      "SELECT + INSERT (read-write)",
			canSelect: true,
			canUpdate: false,
			canInsert: true,
			canDelete: false,
			want:      0600,
		},
		{
			name:      "all privileges",
			canSelect: true,
			canUpdate: true,
			canInsert: true,
			canDelete: true,
			want:      0600,
		},
		{
			name:      "UPDATE only (write-only)",
			canSelect: false,
			canUpdate: true,
			canInsert: false,
			canDelete: false,
			want:      0200,
		},
		{
			name:      "INSERT only (write-only)",
			canSelect: false,
			canUpdate: false,
			canInsert: true,
			canDelete: false,
			want:      0200,
		},
		{
			name:      "DELETE only has no mode effect",
			canSelect: false,
			canUpdate: false,
			canInsert: false,
			canDelete: true,
			want:      0,
		},
		{
			name:      "SELECT + DELETE",
			canSelect: true,
			canUpdate: false,
			canInsert: false,
			canDelete: true,
			want:      0400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MapPermissions(tt.canSelect, tt.canUpdate, tt.canInsert, tt.canDelete)
			if got != tt.want {
				t.Errorf("MapPermissions(%v, %v, %v, %v) = %04o, want %04o",
					tt.canSelect, tt.canUpdate, tt.canInsert, tt.canDelete, got, tt.want)
			}
		})
	}
}

func TestMapDirectoryPermissions(t *testing.T) {
	tests := []struct {
		name      string
		canSelect bool
		canUpdate bool
		canInsert bool
		canDelete bool
		want      os.FileMode
	}{
		{
			name:      "no permissions",
			canSelect: false,
			canUpdate: false,
			canInsert: false,
			canDelete: false,
			want:      0,
		},
		{
			name:      "SELECT only (read + execute)",
			canSelect: true,
			canUpdate: false,
			canInsert: false,
			canDelete: false,
			want:      0500,
		},
		{
			name:      "SELECT + UPDATE (full access)",
			canSelect: true,
			canUpdate: true,
			canInsert: false,
			canDelete: false,
			want:      0700,
		},
		{
			name:      "SELECT + INSERT (full access)",
			canSelect: true,
			canUpdate: false,
			canInsert: true,
			canDelete: false,
			want:      0700,
		},
		{
			name:      "all privileges",
			canSelect: true,
			canUpdate: true,
			canInsert: true,
			canDelete: true,
			want:      0700,
		},
		{
			name:      "UPDATE only (write only)",
			canSelect: false,
			canUpdate: true,
			canInsert: false,
			canDelete: false,
			want:      0200,
		},
		{
			name:      "INSERT only (write only)",
			canSelect: false,
			canUpdate: false,
			canInsert: true,
			canDelete: false,
			want:      0200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MapDirectoryPermissions(tt.canSelect, tt.canUpdate, tt.canInsert, tt.canDelete)
			if got != tt.want {
				t.Errorf("MapDirectoryPermissions(%v, %v, %v, %v) = %04o, want %04o",
					tt.canSelect, tt.canUpdate, tt.canInsert, tt.canDelete, got, tt.want)
			}
		})
	}
}
