package cmd

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

var (
	// Version information (set via ldflags during build)
	Version   = "dev"
	BuildTime = "unknown"
	GitCommit = "unknown"
)

func buildVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Show version information",
		Long:  `Display version, build time, and git commit information.`,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("TigerFS %s\n", Version)
			fmt.Printf("Build time: %s\n", BuildTime)
			fmt.Printf("Git commit: %s\n", GitCommit)
			fmt.Printf("Go version: %s\n", runtime.Version())
			fmt.Printf("Platform: %s/%s\n", runtime.GOOS, runtime.GOARCH)
		},
	}
}
