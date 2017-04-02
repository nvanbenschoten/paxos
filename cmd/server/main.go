package main

import (
	"fmt"
	"log"
	"os"

	"github.com/pkg/errors"
	flag "github.com/spf13/pflag"

	"github.com/nvanbenschoten/paxos/cmd/util"
	"github.com/nvanbenschoten/paxos/paxos"
)

const (
	hostfileDesc = "The hostfile is the path to a file that contains " +
		"the list of hostnames that the servers are " +
		"running on. It assumes that each host is running " +
		"only one instance of the server. It should be " +
		"in the format of a hostname per line. " +
		"The line number indicates the identifier of the server, " +
		"which starts at 0."
	serverPortDesc = "The server_port identifies on which port each server " +
		"will be listening on for incoming TCP connections from " +
		"clients. It can take any integer from 1024 to 65535, " +
		"but must be different from paxos_port."
	idDesc = "The optional id specifier of this process. Only needed if multiple " +
		"processes in the hostfile are running on the same host, otherwise it can " +
		"be deduced from the hostfile. 0-indexed."
	verboseDesc = "Sets the logging level to verbose."
)

var (
	help       = flag.Bool("help", false, "")
	verbose    = flag.BoolP("verbose", "v", false, verboseDesc)
	hostfile   = flag.StringP("hostfile", "h", "hostfile", hostfileDesc)
	serverPort = flag.IntP("server_port", "s", 2346, serverPortDesc)
	hostID     = flag.IntP("id", "i", -1, idDesc)
)

func main() {
	flag.CommandLine.MarkHidden("help")
	flag.Parse()
	if *help {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprint(os.Stderr, flag.CommandLine.FlagUsagesWrapped(120))
		return
	}

	ph, err := parseHostfile()
	if err != nil {
		log.Fatal(err)
	}
	s, err := newServer(ph)
	if err != nil {
		log.Fatal(err)
	}
	s.Run()
}

func parseHostfile() (parsedHostfile, error) {
	var ph parsedHostfile
	if *hostfile == "" {
		return ph, errors.New("hostfile flag required")
	}

	addrs, err := util.ParseHostfile(*hostfile, *serverPort)
	if err != nil {
		return ph, err
	}

	hostIDSet := *hostID >= 0
	myHostname, err := os.Hostname()
	if err != nil {
		return ph, err
	}

	for _, addr := range addrs {
		if (hostIDSet && addr.Idx == *hostID) || (!hostIDSet && addr.Host == myHostname) {
			if hostIDSet && addr.Host != myHostname {
				return ph, errors.Errorf("id flag %d is not the hostname of this host", *hostID)
			}
			if ph.localInfoSet() {
				return ph, errors.Errorf("local host information set twice in hostfile. " +
					"Consider using the --id flag.")
			}
			ph.myID = addr.Idx
			ph.myPort = addr.Port
		} else {
			ph.peerAddrs = append(ph.peerAddrs, addr)
		}
	}

	if !ph.localInfoSet() {
		if *hostID >= len(ph.peerAddrs) {
			return ph, errors.Errorf("--id flag value too large")
		}
		return ph, errors.Errorf("local hostname %q not in hostfile", myHostname)
	}
	return ph, nil
}

type parsedHostfile struct {
	myID      int
	myPort    int
	peerAddrs []util.Addr
}

func (ph parsedHostfile) toPaxosConfig() *paxos.Config {
	logger := paxos.NewDefaultLogger()
	if *verbose {
		logger.EnableDebug()
	}
	return &paxos.Config{
		ID:        uint64(ph.myID),
		NodeCount: uint64(len(ph.peerAddrs) + 1),
		Logger:    logger,
	}
}

func (ph parsedHostfile) localInfoSet() bool {
	return ph.myPort != 0
}
