package tinylb

import (
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// Target manages TCP connections to a backend server.
type Target struct {
	sync.RWMutex

	// addr is the address of the backend server.
	addr *net.TCPAddr
	// timeout is how long to keep inactive connections to the backend open (in seconds).
	timeout time.Duration
	// numConns is the number of currently open connections to this server.
	numConns int

	log *logrus.Entry
}

// newTarget creates a new target with the provided configuration.
func newTarget(config TargetConfig, timeout uint, log *logrus.Entry) (*Target, error) {
	portString := strconv.Itoa(int(config.Port))
	addr, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(config.Host, portString))
	if err != nil {
		return nil, err
	}
	backend := Target{
		addr:    addr,
		timeout: time.Duration(timeout) * time.Second,
		log:     log.WithField("target", addr),
	}

	return &backend, nil
}

// drain blocks until the number of open connections to this target is 0.
func (b *Target) drain() {
	b.log.Info("draining target")
	for {
		b.RLock()
		numConns := b.numConns
		b.RUnlock()
		if numConns == 0 {
			break
		}
		time.Sleep(1 * time.Second)
	}
	b.log.Info("done draining target")
}

// proxy opens a connection to the target's backend and copies data
// bidirectionally between `rconn` and the backend.
func (t *Target) proxy(rconn *net.TCPConn) {
	defer rconn.Close()

	t.log.Info("opening connection from ", rconn.RemoteAddr())

	// Open a connection to the target's backend.
	lconn, err := net.DialTCP("tcp", nil, t.addr)
	if err != nil {
		t.log.Error("failed to connect to backend: ", err)
		return
	}
	defer lconn.Close()

	// Ensure the proxy connections don't live forever.
	rconn.SetDeadline(time.Now().Add(t.timeout))
	lconn.SetDeadline(time.Now().Add(t.timeout))

	t.Lock()
	t.numConns++
	t.Unlock()

	// Copy data between the connections in both directions.
	go t.pipe(rconn, lconn)
	t.pipe(lconn, rconn)

	t.Lock()
	t.numConns--
	t.Unlock()

	t.log.Info("closing connection from ", rconn.RemoteAddr().String())
}

// pipe copies all data from `src` to `dst` until either connection closes
// or an unexpected error occurs.
func (b *Target) pipe(dst *net.TCPConn, src *net.TCPConn) {
	_, err := io.Copy(dst, src)
	// Ignore net.ErrClosed, as this is expected when one proxy
	// TCP connection closes before the other.
	if err != nil && !errors.Is(err, net.ErrClosed) {
		b.log.Error("error copying bytes: ", err)
	}
}

// healthy checks if the target's backend server is healthy by attempting to
// open a TCP connection to it.
func (b *Target) healthy() bool {
	conn, err := net.DialTCP("tcp", nil, b.addr)
	if err != nil {
		return false
	}
	err = conn.Close()
	if err != nil {
		b.log.Error("error closing healtcheck connection: ", err)
	}
	return true
}
