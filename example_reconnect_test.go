package sftp_test

import (
	"log"
	"sync"
	"time"

	"github.com/ad3n/sftp"
	"golang.org/x/crypto/ssh"
)

type reconnectingClient struct {
	addr   string
	config *ssh.ClientConfig

	mu     sync.Mutex
	client *sftp.Client
	done   chan struct{}
}

func (rc *reconnectingClient) Client() (*sftp.Client, error) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.client != nil {
		select {
		case <-rc.done:

		default:

			if _, err := rc.client.RealPath("."); err == nil {
				return rc.client, nil
			}

			rc.client.Close()
		}

		rc.client = nil
	}

	conn, err := ssh.Dial("tcp", rc.addr, rc.config)
	if err != nil {
		return nil, err
	}

	client, err := sftp.NewClient(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}

	done := make(chan struct{})
	go func() {

		err := client.Wait()
		conn.Close()
		close(done)
		log.Print("sftp: disconnected: ", err)
	}()

	rc.client, rc.done = client, done
	return client, nil
}

func (rc *reconnectingClient) Close() error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.client == nil {
		return nil
	}

	err := rc.client.Close()
	<-rc.done
	rc.client = nil

	return err
}

func Example_reconnect() {
	var config *ssh.ClientConfig

	rc := &reconnectingClient{
		addr:   "example.com:22",
		config: config,
	}
	defer rc.Close()

	for range time.Tick(time.Minute) {
		client, err := rc.Client()
		if err != nil {
			log.Print(err)
			continue
		}

		fi, err := client.Stat("/upload/ready")
		if err != nil {
			log.Print(err)
			continue
		}

		log.Print(fi.Name(), " is ready")
	}
}
