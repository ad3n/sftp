package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"github.com/ad3n/sftp"
	"golang.org/x/crypto/ssh"
)

func main() {
	var (
		readOnly    bool
		debugStderr bool
		winRoot     bool
	)

	flag.BoolVar(&readOnly, "R", false, "read-only server")
	flag.BoolVar(&debugStderr, "e", false, "debug to stderr")
	flag.BoolVar(&winRoot, "wr", false, "windows root")

	flag.Parse()

	debugStream := io.Discard
	if debugStderr {
		debugStream = os.Stderr
	}

	config := &ssh.ServerConfig{
		PasswordCallback: func(c ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) {
			fmt.Fprintf(debugStream, "Login: %s\n", c.User())
			if c.User() == "testuser" && string(pass) == "tiger" {
				return nil, nil
			}

			return nil, fmt.Errorf("password rejected for %q", c.User())
		},
	}

	privateBytes, err := os.ReadFile("id_rsa")
	if err != nil {
		log.Fatal("Failed to load private key", err)
	}

	private, err := ssh.ParsePrivateKey(privateBytes)
	if err != nil {
		log.Fatal("Failed to parse private key", err)
	}

	config.AddHostKey(private)

	listener, err := net.Listen("tcp", "0.0.0.0:2022")
	if err != nil {
		log.Fatal("failed to listen for connection", err)
	}

	fmt.Printf("Listening on %v\n", listener.Addr())

	nConn, err := listener.Accept()
	if err != nil {
		log.Fatal("failed to accept incoming connection", err)
	}

	_, chans, reqs, err := ssh.NewServerConn(nConn, config)
	if err != nil {
		log.Fatal("failed to handshake", err)
	}

	fmt.Fprintf(debugStream, "SSH server established\n")

	go ssh.DiscardRequests(reqs)

	for newChannel := range chans {
		fmt.Fprintf(debugStream, "Incoming channel: %s\n", newChannel.ChannelType())
		if newChannel.ChannelType() != "session" {
			newChannel.Reject(ssh.UnknownChannelType, "unknown channel type")
			fmt.Fprintf(debugStream, "Unknown channel type: %s\n", newChannel.ChannelType())
			continue
		}

		channel, requests, err := newChannel.Accept()
		if err != nil {
			log.Fatal("could not accept channel.", err)
		}

		fmt.Fprintf(debugStream, "Channel accepted\n")

		go func(in <-chan *ssh.Request) {
			for req := range in {
				fmt.Fprintf(debugStream, "Request: %v\n", req.Type)
				ok := false
				switch req.Type {
				case "subsystem":
					fmt.Fprintf(debugStream, "Subsystem: %s\n", req.Payload[4:])
					if string(req.Payload[4:]) == "sftp" {
						ok = true
					}
				}

				fmt.Fprintf(debugStream, " - accepted: %v\n", ok)
				req.Reply(ok, nil)
			}
		}(requests)

		serverOptions := []sftp.ServerOption{
			sftp.WithDebug(debugStream),
		}

		switch {
		case readOnly:

			serverOptions = append(serverOptions, sftp.ReadOnly())
			fmt.Fprintf(debugStream, "Read-only server\n")

		default:

			fmt.Fprintf(debugStream, "Read write server\n")
		}

		if winRoot {
			serverOptions = append(serverOptions, sftp.WindowsRootEnumeratesDrives())
			fmt.Fprintf(debugStream, "Windows root enabled\n")
		}

		server, err := sftp.NewServer(
			channel,
			serverOptions...,
		)
		if err != nil {
			log.Fatal(err)
		}

		err = server.Serve()
		if err == io.EOF {
			err = nil
		}

		exitStatus := uint32(0)
		if err != nil {
			exitStatus = 1
		}

		channel.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{exitStatus}))

		server.Close()
		if err != nil {
			log.Fatal("sftp server completed with error:", err)
		}

		log.Print("sftp client exited session.")
	}
}
