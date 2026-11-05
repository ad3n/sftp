package sftp_test

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/ad3n/sftp"
	"golang.org/x/crypto/ssh"
)

func Example() {
	var conn *ssh.Client

	client, err := sftp.NewClient(conn)
	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	w := client.Walk("/home/user")
	for w.Step() {
		if w.Err() != nil {
			continue
		}

		log.Println(w.Path())
	}

	f, err := client.Create("hello.txt")
	if err != nil {
		log.Fatal(err)
	}

	if _, err := f.Write([]byte("Hello world!")); err != nil {
		log.Fatal(err)
	}

	f.Close()

	fi, err := client.Lstat("hello.txt")
	if err != nil {
		log.Fatal(err)
	}

	log.Println(fi)
}

func ExampleNewClientPipe() {
	cmd := exec.Command("ssh", "example.com", "-s", "sftp")

	cmd.Stderr = os.Stderr

	wr, err := cmd.StdinPipe()
	if err != nil {
		log.Fatal(err)
	}

	rd, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}

	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}

	defer cmd.Wait()

	client, err := sftp.NewClientPipe(rd, wr)
	if err != nil {
		log.Fatal(err)
	}

	list, err := client.ReadDir("/")
	if err != nil {
		log.Fatal(err)
	}

	for _, item := range list {
		fmt.Println(item.Name())
	}

	client.Close()
}

func ExampleClient_Mkdir_parents() {
	var conn *ssh.Client

	client, err := sftp.NewClient(conn)
	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	sshFxFailure := uint32(4)
	mkdirParents := func(client *sftp.Client, dir string) (err error) {
		var parents string

		if path.IsAbs(dir) {
			parents = "/"
		}

		for name := range strings.SplitSeq(dir, "/") {
			if name == "" {
				continue
			}

			parents = path.Join(parents, name)
			err = client.Mkdir(parents)
			if status, ok := err.(*sftp.StatusError); ok {
				if status.Code == sshFxFailure {
					var fi os.FileInfo
					fi, err = client.Stat(parents)
					if err == nil {
						if !fi.IsDir() {
							return fmt.Errorf("file exists: %s", parents)
						}
					}
				}
			}

			if err != nil {
				break
			}
		}

		return err
	}

	err = mkdirParents(client, "/tmp/foo/bar")
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleFile_ReadFrom_bufio() {
	var data_source io.Reader
	var f *sftp.File
	type writerOnly struct{ io.Writer }
	bw := bufio.NewWriter(writerOnly{f})
	bw.ReadFrom(data_source)
}

func ExampleNewClientPipe_tls() {
	var clientCert tls.Certificate

	conn, err := tls.Dial("tcp", "example.com:2022", &tls.Config{
		Certificates: []tls.Certificate{clientCert},
	})
	if err != nil {
		log.Fatal(err)
	}

	client, err := sftp.NewClientPipe(conn, conn)
	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	list, err := client.ReadDir("/")
	if err != nil {
		log.Fatal(err)
	}

	for _, item := range list {
		fmt.Println(item.Name())
	}
}

func ExampleNewServer() {
	var serverCert tls.Certificate
	var clientCAs *x509.CertPool

	listener, err := tls.Listen("tcp", ":2022", &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
	})
	if err != nil {
		log.Fatal(err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}

		go func() {
			server, err := sftp.NewServer(conn)
			if err != nil {
				log.Print(err)
				conn.Close()
				return
			}

			if err := server.Serve(); err != nil && err != io.EOF {
				log.Print("sftp server completed with error: ", err)
			}

			conn.Close()
		}()
	}
}
