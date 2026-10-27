// Package pushhack follows Live's tempo through the PushHack Remote Script.
//
// It opens one connection to the script, checks hello, subscribes to the
// "tempo" topic, and calls OnTempo for each change. The script pushes an
// event as soon as Live's tempo changes, so there is no polling and no need
// for Live to send MIDI clock to this hack.
//
// Active reports whether a good connection exists. When it is false (the
// script is not installed, or Live has not loaded it), the caller keeps using
// its older tempo sources.
package pushhack

import (
	"bufio"
	"encoding/json"
	"errors"
	"net"
	"sync/atomic"
	"time"
)

// Addr is the script's TCP endpoint on the Push.
const Addr = "127.0.0.1:7704"

// MinProtocol is the oldest script protocol this client understands.
const MinProtocol = 1

type Client struct {
	Addr    string
	OnTempo func(bpm float64)
	Logf    func(format string, args ...any)

	// Backoff bounds the wait between connection tries. Zero: 2 s to 15 s.
	MinBackoff, MaxBackoff time.Duration
	KeepAlive              time.Duration // quiet time before a hello; zero: 30 s

	active atomic.Bool
	stop   chan struct{}
}

// Active is true while connected, subscribed and the script is new enough.
func (c *Client) Active() bool { return c.active.Load() }

// Start runs the client in the background.
func (c *Client) Start() {
	if c.Addr == "" {
		c.Addr = Addr
	}
	if c.Logf == nil {
		c.Logf = func(string, ...any) {}
	}
	if c.MinBackoff == 0 {
		c.MinBackoff = 2 * time.Second
	}
	if c.MaxBackoff == 0 {
		c.MaxBackoff = 15 * time.Second
	}
	c.stop = make(chan struct{})
	go c.loop()
}

// Stop ends the client. The connection closes within a read timeout.
func (c *Client) Stop() { close(c.stop) }

func (c *Client) loop() {
	wait := c.MinBackoff
	lastErr := ""
	for {
		start := time.Now()
		err := c.session()
		c.active.Store(false)
		// Log a new kind of failure once, not on every try.
		if msg := errString(err); msg != lastErr {
			c.Logf("pushhack: %s", msg)
			lastErr = msg
		}
		if time.Since(start) > 30*time.Second {
			wait = c.MinBackoff // it worked for a while: retry soon
		}
		select {
		case <-c.stop:
			return
		case <-time.After(wait):
		}
		if wait *= 2; wait > c.MaxBackoff {
			wait = c.MaxBackoff
		}
	}
}

func (c *Client) keepAlive() time.Duration {
	if c.KeepAlive > 0 {
		return c.KeepAlive
	}
	return 30 * time.Second
}

func has(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func errString(err error) string {
	if err == nil {
		return "connection closed"
	}
	return err.Error()
}

var errTooOld = errors.New("the PushHack Remote Script is too old")

type message struct {
	ID    int    `json:"id"`
	OK    bool   `json:"ok"`
	Error string `json:"error"`
	T     string `json:"t"`
	Topic string `json:"topic"`
	Data  struct {
		Protocol int      `json:"protocol"`
		Topics   []string `json:"topics"`
		Tempo    float64  `json:"tempo"`
	} `json:"data"`
}

func (c *Client) session() error {
	conn, err := net.DialTimeout("tcp", c.Addr, 3*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	go func() { <-c.stop; conn.Close() }()

	rd := bufio.NewReaderSize(conn, 64<<10)
	call := func(req string) (message, error) {
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Write([]byte(req + "\n")); err != nil {
			return message{}, err
		}
		line, err := rd.ReadBytes('\n')
		if err != nil {
			return message{}, err
		}
		var m message
		if err := json.Unmarshal(line, &m); err != nil {
			// An old PushHackBrowser answers plain text, not JSON.
			return m, errors.New("the Remote Script on this Push is not PushHack")
		}
		return m, nil
	}

	hello, err := call(`{"id":1,"cmd":"hello"}`)
	if err != nil {
		return err
	}
	if !hello.OK || hello.Data.Protocol < MinProtocol || !has(hello.Data.Topics, "tempo") {
		return errTooOld
	}
	sub, err := call(`{"id":2,"cmd":"subscribe","topics":["tempo"]}`)
	if err != nil {
		return err
	}
	if !sub.OK {
		return errors.New("subscribe refused: " + sub.Error)
	}
	c.active.Store(true)
	c.Logf("pushhack: following Live's tempo")

	for {
		// The script sends an event only when the tempo changes, so a quiet
		// socket is normal. Ask hello now and then: a dead socket fails the write.
		_ = conn.SetReadDeadline(time.Now().Add(c.keepAlive()))
		line, err := rd.ReadBytes('\n')
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
				if _, werr := conn.Write([]byte(`{"id":3,"cmd":"hello"}` + "\n")); werr != nil {
					return werr
				}
				continue
			}
			return err
		}
		var m message
		if json.Unmarshal(line, &m) != nil || m.T != "event" || m.Topic != "tempo" {
			continue
		}
		if m.Data.Tempo > 0 && c.OnTempo != nil {
			c.OnTempo(m.Data.Tempo)
		}
	}
}
