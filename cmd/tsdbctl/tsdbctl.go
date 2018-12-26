package main

import (
	"os"

	"github.com/gtopper/v3io-tsdb/pkg/tsdbctl"
)

func main() {
	if err := Run(); err != nil {
		os.Exit(1)
	}
	os.Exit(0)
}

func Run() error {
	rootCmd := tsdbctl.NewRootCommandeer()
	defer tearDown(rootCmd)
	return rootCmd.Execute()
}

func tearDown(cmd *tsdbctl.RootCommandeer) {
	if cmd.Reporter != nil { // could be nil if has failed on initialisation
		cmd.Reporter.Stop()
	}
}
