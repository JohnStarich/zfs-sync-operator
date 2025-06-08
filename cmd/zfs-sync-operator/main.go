// Command zfs-sync-operator runs the ZFS offsite backup operator. See the README for more details.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/johnstarich/zfs-sync-operator/internal/operator"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	err := operator.Run(ctx, os.Args[1:], os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
