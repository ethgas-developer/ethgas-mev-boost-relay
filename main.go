package main

import (
	"bitbucket.org/infinity-exchange/mev-boost-relay/cmd"
)

var Version = "dev" // is set during build process

func main() {
	cmd.Version = Version
	cmd.Execute()
}
