package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	err := run(ctx, os.Args[1:], os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(_ context.Context, args []string, out io.Writer) error {
	flagSet := flag.NewFlagSet("", flag.ContinueOnError)
	err := flagSet.Parse(args)
	if err != nil {
		return err
	}
	if len(flagSet.Args()) > 0 {
		return errors.New("zfs-sync-operator: this command does not take any arguments")
	}

	fmt.Fprintln(out, "Ready!")
	return nil
}
