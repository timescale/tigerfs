package nfs

import (
	"testing"

	"github.com/go-git/go-billy/v5"
	nfshelper "github.com/willscott/go-nfs/helpers"
)

// TestOpsFilesystemImplementsChange verifies that OpsFilesystem implements billy.Change.
func TestOpsFilesystemImplementsChange(t *testing.T) {
	// Create a minimal OpsFilesystem (ops can be nil for this test)
	fs := &OpsFilesystem{}

	// Test that it implements billy.Change
	if _, ok := any(fs).(billy.Change); !ok {
		t.Fatal("OpsFilesystem does not implement billy.Change")
	}

	// Test that NullAuthHandler.Change() returns non-nil
	handler := nfshelper.NewNullAuthHandler(fs)

	// Access the Change method through a type assertion
	type changer interface {
		Change(billy.Filesystem) billy.Change
	}
	if ch, ok := handler.(changer); !ok {
		t.Fatal("handler does not have Change method")
	} else {
		change := ch.Change(fs)
		if change == nil {
			t.Fatal("NullAuthHandler.Change() returned nil - billy.Change not recognized")
		}
		t.Logf("NullAuthHandler.Change() returned: %T", change)
	}
}

// TestOpsChrootFilesystemImplementsChange verifies that opsChrootFilesystem implements billy.Change.
func TestOpsChrootFilesystemImplementsChange(t *testing.T) {
	// Create via Chroot
	fs := &OpsFilesystem{}
	chrooted, err := fs.Chroot("/test")
	if err != nil {
		t.Fatalf("Chroot failed: %v", err)
	}

	// Test that it implements billy.Change
	if _, ok := chrooted.(billy.Change); !ok {
		t.Fatal("opsChrootFilesystem does not implement billy.Change")
	}
}
