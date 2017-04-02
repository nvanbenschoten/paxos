package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	flag "github.com/spf13/pflag"

	"github.com/nvanbenschoten/paxos/cmd/util"
)

const (
	hostfileDesc = "The hostfile is the path to a file that contains " +
		"the list of hostnames that the servers are running on. It should " +
		"be in the format of a hostname per line. The line number indicates " +
		"the identifier of the server, which starts at 0."
	portDesc = "The port identifies on which port each server " +
		"will be listening on for incoming TCP connections from " +
		"clients. It can take any integer from 1024 to 65535."
)

var (
	help     = flag.Bool("help", false, "")
	hostfile = flag.StringP("hostfile", "h", "hostfile", hostfileDesc)
	port     = flag.IntP("port", "p", 2346, portDesc)
)

func main() {
	rand.Seed(time.Now().UTC().UnixNano())
	flag.CommandLine.MarkHidden("help")
	flag.Parse()
	if *help {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprint(os.Stderr, flag.CommandLine.FlagUsagesWrapped(120))
		return
	}

	if *hostfile == "" {
		log.Fatal("hostfile flag required")
	}

	addrs, err := util.ParseHostfile(*hostfile, *port)
	if err != nil {
		log.Fatal(err)
	}

	client, err := newClient(addrs)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Enter an update: ")
		update, err := reader.ReadBytes('\n')
		if err != nil {
			log.Fatal(err)
		}
		gou, err := client.sendUpdate(ctx, update[:len(update)-1])
		if err != nil {
			log.Println(err)
			continue
		}
		fmt.Printf("Ordered update: %v\n", gou)
	}
}
