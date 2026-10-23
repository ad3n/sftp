package sftp

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	crand "crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kr/fs"
	"github.com/stretchr/testify/assert"
	"golang.org/x/crypto/ssh"
)

const (
	GolangSFTP = true

	OpenSSHSFTP = false
)

var (
	testServerImpl = flag.Bool("testserver", false, "perform integration tests against sftp package server instance")

	testIntegration = flag.Bool("integration", false, "perform integration tests against sftp server process")

	testAllocator = flag.Bool("allocator", false, "perform tests using the allocator")

	testSftp *string

	testSftpClientBin *string

	sshServerDebugStream = io.Discard

	sftpServerDebugStream = io.Discard

	sftpClientDebugStream = io.Discard

	hostPrivateKeySigner ssh.Signer

	privKey = []byte(`
-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEArhp7SqFnXVZAgWREL9Ogs+miy4IU/m0vmdkoK6M97G9NX/Pj
wf8I/3/ynxmcArbt8Rc4JgkjT2uxx/NqR0yN42N1PjO5Czu0dms1PSqcKIJdeUBV
7gdrKSm9Co4d2vwfQp5mg47eG4w63pz7Drk9+VIyi9YiYH4bve7WnGDswn4ycvYZ
slV5kKnjlfCdPig+g5P7yQYud0cDWVwyA0+kxvL6H3Ip+Fu8rLDZn4/P1WlFAIuc
PAf4uEKDGGmC2URowi5eesYR7f6GN/HnBs2776laNlAVXZUmYTUfOGagwLsEkx8x
XdNqntfbs2MOOoK+myJrNtcB9pCrM0H6um19uQIDAQABAoIBABkWr9WdVKvalgkP
TdQmhu3mKRNyd1wCl+1voZ5IM9Ayac/98UAvZDiNU4Uhx52MhtVLJ0gz4Oa8+i16
IkKMAZZW6ro/8dZwkBzQbieWUFJ2Fso2PyvB3etcnGU8/Yhk9IxBDzy+BbuqhYE2
1ebVQtz+v1HvVZzaD11bYYm/Xd7Y28QREVfFen30Q/v3dv7dOteDE/RgDS8Czz7w
jMW32Q8JL5grz7zPkMK39BLXsTcSYcaasT2ParROhGJZDmbgd3l33zKCVc1zcj9B
SA47QljGd09Tys958WWHgtj2o7bp9v1Ufs4LnyKgzrB80WX1ovaSQKvd5THTLchO
kLIhUAECgYEA2doGXy9wMBmTn/hjiVvggR1aKiBwUpnB87Hn5xCMgoECVhFZlT6l
WmZe7R2klbtG1aYlw+y+uzHhoVDAJW9AUSV8qoDUwbRXvBVlp+In5wIqJ+VjfivK
zgIfzomL5NvDz37cvPmzqIeySTowEfbQyq7CUQSoDtE9H97E2wWZhDkCgYEAzJdJ
k+NSFoTkHhfD3L0xCDHpRV3gvaOeew8524fVtVUq53X8m91ng4AX1r74dCUYwwiF
gqTtSSJfx2iH1xKnNq28M9uKg7wOrCKrRqNPnYUO3LehZEC7rwUr26z4iJDHjjoB
uBcS7nw0LJ+0Zeg1IF+aIdZGV3MrAKnrzWPixYECgYBsffX6ZWebrMEmQ89eUtFF
u9ZxcGI/4K8ErC7vlgBD5ffB4TYZ627xzFWuBLs4jmHCeNIJ9tct5rOVYN+wRO1k
/CRPzYUnSqb+1jEgILL6istvvv+DkE+ZtNkeRMXUndWwel94BWsBnUKe0UmrSJ3G
sq23J3iCmJW2T3z+DpXbkQKBgQCK+LUVDNPE0i42NsRnm+fDfkvLP7Kafpr3Umdl
tMY474o+QYn+wg0/aPJIf9463rwMNyyhirBX/k57IIktUdFdtfPicd2MEGETElWv
nN1GzYxD50Rs2f/jKisZhEwqT9YNyV9DkgDdGGdEbJNYqbv0qpwDIg8T9foe8E1p
bdErgQKBgAt290I3L316cdxIQTkJh1DlScN/unFffITwu127WMr28Jt3mq3cZpuM
Aecey/eEKCj+Rlas5NDYKsB18QIuAw+qqWyq0LAKLiAvP1965Rkc4PLScl3MgJtO
QYa37FK0p8NcDeUuF86zXBVutwS5nJLchHhKfd590ks57OROtm29
-----END RSA PRIVATE KEY-----
`)

	rng = rand.New(rand.NewSource(time.Now().Unix()))
)

type (
	sshServer struct {
		useSubsystem bool
		conn         net.Conn
		config       *ssh.ServerConfig
		sshConn      *ssh.ServerConn
		newChans     <-chan ssh.NewChannel
		newReqs      <-chan *ssh.Request
	}

	sshChannelServer struct {
		svr     *sshServer
		chanReq ssh.NewChannel
		ch      ssh.Channel
		newReqs <-chan *ssh.Request
	}

	sshSessionChannelServer struct {
		*sshChannelServer
		env []string
	}

	sshEnvRequest struct {
		Envvar string
		Value  string
	}

	sshSubsystemRequest struct {
		Name string
	}

	sshSubsystemExitStatus struct {
		Status uint32
	}

	execError struct {
		path   string
		stderr string
		err    error
	}
)

func TestMain(m *testing.M) {
	sftpClientLocation, _ := exec.LookPath("sftp")
	testSftpClientBin = flag.String("sftp_client", sftpClientLocation, "location of the sftp client binary")

	lookSFTPServer := []string{
		"/usr/libexec/sftp-server",
		"/usr/lib/openssh/sftp-server",
		"/usr/lib/ssh/sftp-server",
		"C:\\Program Files\\Git\\usr\\lib\\ssh\\sftp-server.exe",
	}
	sftpServer, _ := exec.LookPath("sftp-server")
	if len(sftpServer) == 0 {
		for _, location := range lookSFTPServer {
			if _, err := os.Stat(location); err == nil {
				sftpServer = location
				break
			}
		}
	}

	testSftp = flag.String("sftp", sftpServer, "location of the sftp server binary")
	flag.Parse()

	os.Exit(m.Run())
}

func skipIfWindows(t testing.TB) {
	if runtime.GOOS == "windows" {
		t.Skip("skipping test on windows")
	}
}

func skipIfPlan9(t testing.TB) {
	if runtime.GOOS == "plan9" {
		t.Skip("skipping test on plan9")
	}
}

func init() {
	var err error
	hostPrivateKeySigner, err = ssh.ParsePrivateKey(privKey)
	if err != nil {
		panic(err)
	}
}

func keyAuth(conn ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
	permissions := &ssh.Permissions{
		CriticalOptions: map[string]string{},
		Extensions:      map[string]string{},
	}
	return permissions, nil
}

func pwAuth(conn ssh.ConnMetadata, pw []byte) (*ssh.Permissions, error) {
	permissions := &ssh.Permissions{
		CriticalOptions: map[string]string{},
		Extensions:      map[string]string{},
	}
	return permissions, nil
}

func basicServerConfig() *ssh.ServerConfig {
	config := ssh.ServerConfig{
		MACs:              []string{"hmac-sha1"},
		PasswordCallback:  pwAuth,
		PublicKeyCallback: keyAuth,
	}
	config.AddHostKey(hostPrivateKeySigner)
	return &config
}

func sshServerFromConn(conn net.Conn, useSubsystem bool, config *ssh.ServerConfig) (*sshServer, error) {
	sshConn, newChans, newReqs, err := ssh.NewServerConn(conn, config)
	if err != nil {
		return nil, err
	}

	svr := &sshServer{useSubsystem, conn, config, sshConn, newChans, newReqs}
	svr.listenChannels()
	return svr, nil
}

func (svr *sshServer) Wait() error {
	return svr.sshConn.Wait()
}

func (svr *sshServer) Close() error {
	return svr.sshConn.Close()
}

func (svr *sshServer) listenChannels() {
	go func() {
		for chanReq := range svr.newChans {
			go svr.handleChanReq(chanReq)
		}
	}()
	go func() {
		for req := range svr.newReqs {
			go svr.handleReq(req)
		}
	}()
}

func (svr *sshServer) handleReq(req *ssh.Request) {
	switch req.Type {
	default:
		rejectRequest(req)
	}
}

func (svr *sshServer) handleChanReq(chanReq ssh.NewChannel) {
	fmt.Fprintf(sshServerDebugStream, "channel request: %v, extra: '%v'\n", chanReq.ChannelType(), hex.EncodeToString(chanReq.ExtraData()))
	switch chanReq.ChannelType() {
	case "session":
		switch ch, reqs, err := chanReq.Accept(); {
		case err != nil:

			fmt.Fprintf(sshServerDebugStream, "fail to accept channel request: %v\n", err)
			chanReq.Reject(ssh.ResourceShortage, "channel accept failure")

		default:

			chsvr := &sshSessionChannelServer{
				sshChannelServer: &sshChannelServer{svr, chanReq, ch, reqs},
				env:              append([]string{}, os.Environ()...),
			}
			chsvr.handle()
		}
	default:
		chanReq.Reject(ssh.UnknownChannelType, "channel type is not a session")
	}
}

func (chsvr *sshSessionChannelServer) handle() {
	go chsvr.handleReqs()
}

func (chsvr *sshSessionChannelServer) handleReqs() {
	for req := range chsvr.newReqs {
		chsvr.handleReq(req)
	}

	fmt.Fprintf(sshServerDebugStream, "ssh server session channel complete\n")
}

func (chsvr *sshSessionChannelServer) handleReq(req *ssh.Request) {
	switch req.Type {
	case "env":
		chsvr.handleEnv(req)
	case "subsystem":
		chsvr.handleSubsystem(req)
	default:
		rejectRequest(req)
	}
}

func rejectRequest(req *ssh.Request) error {
	fmt.Fprintf(sshServerDebugStream, "ssh rejecting request, type: %s\n", req.Type)
	err := req.Reply(false, []byte{})
	if err != nil {
		fmt.Fprintf(sshServerDebugStream, "ssh request reply had error: %v\n", err)
	}

	return err
}

func rejectRequestUnmarshalError(req *ssh.Request, s any, err error) error {
	fmt.Fprintf(sshServerDebugStream, "ssh request unmarshaling error, type '%T': %v\n", s, err)
	rejectRequest(req)
	return err
}

func (chsvr *sshSessionChannelServer) handleEnv(req *ssh.Request) error {
	envReq := &sshEnvRequest{}
	if err := ssh.Unmarshal(req.Payload, envReq); err != nil {
		return rejectRequestUnmarshalError(req, envReq, err)
	}

	req.Reply(true, nil)

	found := false
	for i, envstr := range chsvr.env {
		if strings.HasPrefix(envstr, envReq.Envvar+"=") {
			found = true
			chsvr.env[i] = envReq.Envvar + "=" + envReq.Value
		}
	}

	if !found {
		chsvr.env = append(chsvr.env, envReq.Envvar+"="+envReq.Value)
	}

	return nil
}

func (chsvr *sshSessionChannelServer) handleSubsystem(req *ssh.Request) error {
	defer func() {
		err1 := chsvr.ch.CloseWrite()
		err2 := chsvr.ch.Close()
		fmt.Fprintf(sshServerDebugStream, "ssh server subsystem request complete, err: %v %v\n", err1, err2)
	}()

	subsystemReq := &sshSubsystemRequest{}
	if err := ssh.Unmarshal(req.Payload, subsystemReq); err != nil {
		return rejectRequestUnmarshalError(req, subsystemReq, err)
	}

	if subsystemReq.Name != "sftp" {
		return req.Reply(false, nil)
	}

	req.Reply(true, nil)

	if !chsvr.svr.useSubsystem {
		cmd := exec.Command(*testSftp, "-e", "-l", "DEBUG")
		cmd.Stdin = chsvr.ch
		cmd.Stdout = chsvr.ch
		cmd.Stderr = sftpServerDebugStream
		if err := cmd.Start(); err != nil {
			return err
		}

		return cmd.Wait()
	}

	sftpServer, err := NewServer(
		chsvr.ch,
		WithDebug(sftpServerDebugStream),
	)
	if err != nil {
		return err
	}

	runErr := sftpServer.Serve()
	exitStatus := uint32(1)
	if runErr == nil {
		exitStatus = uint32(0)
	}

	_, exitStatusErr := chsvr.ch.SendRequest("exit-status", false, ssh.Marshal(sshSubsystemExitStatus{exitStatus}))
	return exitStatusErr
}

func testServer(t *testing.T, useSubsystem bool, readonly bool) (func(), string, int) {
	t.Helper()

	if !*testIntegration {
		t.Skip("skipping integration test")
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	host, portStr, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatal(err)
	}

	shutdown := make(chan struct{})

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-shutdown:
				default:
					t.Error("ssh server socket closed:", err)
				}

				return
			}

			go func() {
				defer conn.Close()

				sshSvr, err := sshServerFromConn(conn, useSubsystem, basicServerConfig())
				if err != nil {
					t.Error(err)
					return
				}

				_ = sshSvr.Wait()
			}()
		}
	}()

	return func() { close(shutdown); listener.Close() }, host, port
}

func makeDummyKey() (string, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), crand.Reader)
	if err != nil {
		return "", fmt.Errorf("cannot generate key: %w", err)
	}

	der, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return "", fmt.Errorf("cannot marshal key: %w", err)
	}

	block := &pem.Block{Type: "EC PRIVATE KEY", Bytes: der}
	f, err := os.CreateTemp("", "sftp-test-key-")
	if err != nil {
		return "", fmt.Errorf("cannot create temp file: %w", err)
	}
	defer func() {
		if f != nil {
			_ = f.Close()
			_ = os.Remove(f.Name())
		}
	}()

	if err := pem.Encode(f, block); err != nil {
		return "", fmt.Errorf("cannot write key: %w", err)
	}

	if err := f.Close(); err != nil {
		return "", fmt.Errorf("error closing key file: %w", err)
	}

	path := f.Name()
	f = nil
	return path, nil
}

func (e *execError) Error() string {
	return fmt.Sprintf("%s: %v: %s", e.path, e.err, e.stderr)
}

func (e *execError) Unwrap() error {
	return e.err
}

func (e *execError) Cause() error {
	return e.err
}

func runSftpClient(t *testing.T, script string, path string, host string, port int) (string, error) {
	if _, err := os.Stat(*testSftpClientBin); err != nil {
		t.Skip("sftp client binary unavailable")
	}

	dummyKey, err := makeDummyKey()
	if err != nil {
		return "", err
	}
	defer os.Remove(dummyKey)

	cmd := exec.Command(
		*testSftpClientBin,

		"-b", "-",
		"-o", "StrictHostKeyChecking=no",
		"-o", "LogLevel=ERROR",
		"-o", "UserKnownHostsFile /dev/null",

		"-o", "IdentityFile="+dummyKey,
		"-o", "IdentitiesOnly=yes",
		"-P", fmt.Sprintf("%d", port), fmt.Sprintf("%s:%s", host, path),
	)

	cmd.Stdin = strings.NewReader(script)

	stdout := new(bytes.Buffer)
	cmd.Stdout = stdout

	stderr := new(bytes.Buffer)
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		return "", err
	}

	if err := cmd.Wait(); err != nil {
		return stdout.String(), &execError{
			path:   cmd.Path,
			stderr: stderr.String(),
			err:    err,
		}
	}

	return stdout.String(), nil
}

func waitForCondition(t *testing.T, condition func() bool) {
	start := time.Now()
	tick := 10 * time.Millisecond
	waitFor := 100 * time.Millisecond
	for !condition() {
		time.Sleep(tick)
		if time.Since(start) > waitFor {
			break
		}
	}

	assert.True(t, condition())
}

func checkAllocatorBeforeServerClose(t *testing.T, alloc *allocator) {
	if alloc != nil {
		waitForCondition(t, func() bool { return alloc.countUsedPages() <= 1 })
	}
}

func checkAllocatorAfterServerClose(t *testing.T, alloc *allocator) {
	if alloc != nil {
		waitForCondition(t, func() bool { return alloc.countUsedPages() == 0 })
		waitForCondition(t, func() bool { return alloc.countAvailablePages() == 0 })
	}
}

func TestServerCompareSubsystems(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip()
	}

	shutdownGo, hostGo, portGo := testServer(t, GolangSFTP, READONLY)
	defer shutdownGo()

	shutdownOp, hostOp, portOp := testServer(t, OpenSSHSFTP, READONLY)
	defer shutdownOp()

	script := `
ls /
ls -l /
ls /dev/
ls -l /dev/
ls -l /etc/
ls -l /bin/
ls -l /usr/bin/
`
	outputGo, err := runSftpClient(t, script, "/", hostGo, portGo)
	if err != nil {
		t.Fatal(err)
	}

	outputOp, err := runSftpClient(t, script, "/", hostOp, portOp)
	if err != nil {
		t.Fatal(err)
	}

	newlineRegex := regexp.MustCompile(`\r*\n`)
	spaceRegex := regexp.MustCompile(`\s+`)
	outputGoLines := newlineRegex.Split(outputGo, -1)
	outputOpLines := newlineRegex.Split(outputOp, -1)

	if len(outputGoLines) != len(outputOpLines) {
		t.Fatalf("output line count differs, go = %d, openssh = %d", len(outputGoLines), len(outputOpLines))
	}

	for i, goLine := range outputGoLines {
		opLine := outputOpLines[i]
		bad := false
		if goLine != opLine {
			goWords := spaceRegex.Split(goLine, -1)
			opWords := spaceRegex.Split(opLine, -1)

			for j, goWord := range goWords {
				if j >= len(opWords) {
					bad = true
					break
				}

				opWord := opWords[j]
				if goWord != opWord {
					switch j {
					case 1, 2, 3, 7:

					case 8:

						bad = !strings.HasSuffix(opWord, "/"+goWord)
					default:
						bad = true
					}
				}
			}
		}

		if bad {
			t.Errorf("outputs differ\n     go: %q\nopenssh: %q\n", goLine, opLine)
		}
	}
}

func randData(length int) []byte {
	data := make([]byte, length)
	for i := range length {
		data[i] = byte(rng.Uint32())
	}

	return data
}

func randName() string {
	return "sftp." + hex.EncodeToString(randData(16))
}

func TestServerMkdirRmdir(t *testing.T) {
	shutdown, hostGo, portGo := testServer(t, GolangSFTP, READONLY)
	defer shutdown()

	tmpDir := "/tmp/" + randName()
	defer os.RemoveAll(tmpDir)

	if _, err := runSftpClient(t, "mkdir "+tmpDir, "/", hostGo, portGo); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(tmpDir); err != nil {
		t.Fatal(err)
	}

	if _, err := runSftpClient(t, "rmdir "+tmpDir, "/", hostGo, portGo); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(tmpDir); err == nil {
		t.Fatal("should have error after deleting the directory")
	}
}

func TestServerLink(t *testing.T) {
	skipIfWindows(t)
	shutdown, hostGo, portGo := testServer(t, GolangSFTP, READONLY)
	defer shutdown()

	tmpFileLocalData := randData(999)

	linkdest := "/tmp/" + randName()
	defer os.RemoveAll(linkdest)

	if err := os.WriteFile(linkdest, tmpFileLocalData, 0644); err != nil {
		t.Fatal(err)
	}

	link := "/tmp/" + randName()
	defer os.RemoveAll(link)

	if output, err := runSftpClient(t, fmt.Sprintf("ln %s %s", linkdest, link), "/", hostGo, portGo); err != nil {
		t.Fatalf("failed: %v %v", err, string(output))
	}

	{
		stat, err := os.Lstat(link)
		if err != nil {
			t.Fatal(err)
		}

		if int(stat.Size()) != len(tmpFileLocalData) {
			t.Fatalf("wrong size: %v", len(tmpFileLocalData))
		}
	}
}

func TestServerSymlink(t *testing.T) {
	skipIfWindows(t)
	shutdown, hostGo, portGo := testServer(t, GolangSFTP, READONLY)
	defer shutdown()

	link := "/tmp/" + randName()
	defer os.RemoveAll(link)

	if output, err := runSftpClient(t, "symlink /bin/sh "+link, "/", hostGo, portGo); err != nil {
		t.Fatalf("failed: %v %v", err, string(output))
	}

	{
		stat, err := os.Lstat(link)
		if err != nil {
			t.Fatal(err)
		}

		if (stat.Mode() & os.ModeSymlink) != os.ModeSymlink {
			t.Fatalf("is not a symlink: %v", stat.Mode())
		}
	}
}

func TestServerPut(t *testing.T) {
	shutdown, hostGo, portGo := testServer(t, GolangSFTP, READONLY)
	defer shutdown()

	tmpFileLocal := "/tmp/" + randName()
	tmpFileRemote := "/tmp/" + randName()
	defer os.RemoveAll(tmpFileLocal)

	defer os.RemoveAll(tmpFileRemote)

	t.Logf("put: local %v remote %v", tmpFileLocal, tmpFileRemote)

	tmpFileLocalData := randData(10 * 1024 * 1024)
	if err := os.WriteFile(tmpFileLocal, tmpFileLocalData, 0644); err != nil {
		t.Fatal(err)
	}

	if output, err := runSftpClient(t, "put "+tmpFileLocal+" "+tmpFileRemote, "/", hostGo, portGo); err != nil {
		t.Fatalf("runSftpClient failed: %v, output\n%v\n", err, output)
	}

	{
		tmpFileRemoteData, err := os.ReadFile(tmpFileRemote)
		if err != nil {
			t.Fatal(err)
		}

		if string(tmpFileLocalData) != string(tmpFileRemoteData) {
			t.Fatal("contents of file incorrect after put")
		}
	}
}

func TestServerResume(t *testing.T) {
	shutdown, hostGo, portGo := testServer(t, GolangSFTP, READONLY)
	defer shutdown()

	tmpFileLocal := "/tmp/" + randName()
	tmpFileRemote := "/tmp/" + randName()
	defer os.RemoveAll(tmpFileLocal)

	defer os.RemoveAll(tmpFileRemote)

	t.Logf("put: local %v remote %v", tmpFileLocal, tmpFileRemote)

	tmpFileLocalData := randData(2 * 1024 * 1024)

	half := 1024 * 1024
	err := os.WriteFile(tmpFileLocal, tmpFileLocalData[:half], 0644)
	if err != nil {
		t.Fatal(err)
	}

	output, err := runSftpClient(t, "put "+tmpFileLocal+" "+tmpFileRemote,
		"/", hostGo, portGo)
	if err != nil {
		t.Fatalf("runSftpClient failed: %v, output\n%v\n", err, output)
	}

	err = os.WriteFile(tmpFileLocal, tmpFileLocalData, 0644)
	if err != nil {
		t.Fatal(err)
	}

	output, err = runSftpClient(t, "put -a "+tmpFileLocal+" "+tmpFileRemote,
		"/", hostGo, portGo)
	if err != nil {
		t.Fatalf("runSftpClient failed: %v, output\n%v\n", err, output)
	}

	{
		tmpFileRemoteData, err := os.ReadFile(tmpFileRemote)
		if err != nil {
			t.Fatal(err)
		}

		if string(tmpFileLocalData) != string(tmpFileRemoteData) {
			t.Fatal("contents of file incorrect after put")
		}
	}
}

func TestServerGet(t *testing.T) {
	shutdown, hostGo, portGo := testServer(t, GolangSFTP, READONLY)
	defer shutdown()

	tmpFileLocal := "/tmp/" + randName()
	tmpFileRemote := "/tmp/" + randName()
	defer os.RemoveAll(tmpFileLocal)

	defer os.RemoveAll(tmpFileRemote)

	t.Logf("get: local %v remote %v", tmpFileLocal, tmpFileRemote)

	tmpFileRemoteData := randData(10 * 1024 * 1024)
	if err := os.WriteFile(tmpFileRemote, tmpFileRemoteData, 0644); err != nil {
		t.Fatal(err)
	}

	if output, err := runSftpClient(t, "get "+tmpFileRemote+" "+tmpFileLocal, "/", hostGo, portGo); err != nil {
		t.Fatalf("runSftpClient failed: %v, output\n%v\n", err, output)
	}

	{
		tmpFileLocalData, err := os.ReadFile(tmpFileLocal)
		if err != nil {
			t.Fatal(err)
		}

		if string(tmpFileLocalData) != string(tmpFileRemoteData) {
			t.Fatal("contents of file incorrect after put")
		}
	}
}

func compareDirectoriesRecursive(t *testing.T, aroot, broot string) {
	walker := fs.Walk(aroot)
	for walker.Step() {
		if err := walker.Err(); err != nil {
			t.Fatal(err)
		}

		aPath := walker.Path()
		aRel, err := filepath.Rel(aroot, aPath)
		if err != nil {
			t.Fatalf("could not find relative path for %v: %v", aPath, err)
		}

		bPath := filepath.Join(broot, aRel)

		if aRel == "." {
			continue
		}

		aLink, err := os.Lstat(aPath)
		if err != nil {
			t.Fatalf("could not lstat %v: %v", aPath, err)
		}

		if aLink.Mode()&os.ModeSymlink != 0 {
			continue
		}

		aFile, err := os.Stat(aPath)
		if err != nil {
			t.Fatalf("could not stat %v: %v", aPath, err)
		}

		bFile, err := os.Stat(bPath)
		if err != nil {
			t.Fatalf("could not stat %v: %v", bPath, err)
		}

		if aFile.Mode() != bFile.Mode() {
			t.Fatalf("modes different for %v: %v vs %v", aRel, aFile.Mode(), bFile.Mode())
		}

		if !aFile.IsDir() {
			if aFile.Size() != bFile.Size() {
				t.Fatalf("sizes different for %v: %v vs %v", aRel, aFile.Size(), bFile.Size())
			}
		}

		timeDiff := aFile.ModTime().Sub(bFile.ModTime())
		if timeDiff > time.Second || timeDiff < -time.Second {
			t.Fatalf("mtimes different for %v: %v vs %v", aRel, aFile.ModTime(), bFile.ModTime())
		}

		if !aFile.IsDir() {
			{
				aContents, err := os.ReadFile(aPath)
				if err != nil {
					t.Fatal(err)
				}

				{
					bContents, err := os.ReadFile(bPath)
					if err != nil {
						t.Fatal(err)
					}

					if string(aContents) != string(bContents) {
						t.Fatalf("contents different for %v", aRel)
					}
				}
			}
		}
	}
}

func TestServerPutRecursive(t *testing.T) {
	shutdown, hostGo, portGo := testServer(t, GolangSFTP, READONLY)
	defer shutdown()

	dirLocal, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	tmpDirRemote := "/tmp/" + randName()
	defer os.RemoveAll(tmpDirRemote)

	t.Logf("put recursive: local %v remote %v", dirLocal, tmpDirRemote)

	winFix := ""
	if runtime.GOOS == "windows" {
		winFix = "/" + filepath.Base(dirLocal)
	}

	if output, err := runSftpClient(t, "mkdir "+tmpDirRemote+"\r\nput -R -p "+dirLocal+" "+tmpDirRemote+winFix, "/", hostGo, portGo); err != nil {
		t.Fatalf("runSftpClient failed: %v, output\n%v\n", err, output)
	}

	compareDirectoriesRecursive(t, dirLocal, filepath.Join(tmpDirRemote, filepath.Base(dirLocal)))
}

func TestServerGetRecursive(t *testing.T) {
	shutdown, hostGo, portGo := testServer(t, GolangSFTP, READONLY)
	defer shutdown()

	dirRemote, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	tmpDirLocal := "/tmp/" + randName()
	defer os.RemoveAll(tmpDirLocal)

	t.Logf("get recursive: local %v remote %v", tmpDirLocal, dirRemote)

	winFix := ""
	if runtime.GOOS == "windows" {
		winFix = "/" + filepath.Base(dirRemote)
	}

	if output, err := runSftpClient(t, "lmkdir "+tmpDirLocal+"\r\nget -R -p "+dirRemote+" "+tmpDirLocal+winFix, "/", hostGo, portGo); err != nil {
		t.Fatalf("runSftpClient failed: %v, output\n%v\n", err, output)
	}

	compareDirectoriesRecursive(t, dirRemote, filepath.Join(tmpDirLocal, filepath.Base(dirRemote)))
}
