package pushhack

import (
	"bufio"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeScript is a PushHack stand-in: it answers hello and subscribe, then
// sends what the test gives it.
type fakeScript struct {
	ln     net.Listener
	hello  string
	conns  chan net.Conn
	mu     sync.Mutex
	hellos int
	subbed []string
}

func newFake(t *testing.T, hello string) *fakeScript {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeScript{ln: ln, hello: hello, conns: make(chan net.Conn, 4)}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(c)
		}
	}()
	return f
}

func (f *fakeScript) serve(c net.Conn) {
	rd := bufio.NewReader(c)
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			return
		}
		switch {
		case strings.Contains(line, `"hello"`):
			f.mu.Lock()
			f.hellos++
			f.mu.Unlock()
			c.Write([]byte(f.hello + "\n"))
		case strings.Contains(line, `"subscribe"`):
			f.mu.Lock()
			f.subbed = append(f.subbed, line)
			f.mu.Unlock()
			c.Write([]byte(`{"ok":true,"id":2,"data":{"topics":["tempo"]}}` + "\n"))
			f.conns <- c
		}
	}
}

const goodHello = `{"ok":true,"id":1,"data":{"protocol":1,"script_version":"0.2.0","namespaces":["transport"],"topics":["tempo","transport"]}}`

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	end := time.Now().Add(3 * time.Second)
	for time.Now().Before(end) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out: %s", what)
}

type tempos struct {
	mu  sync.Mutex
	got []float64
}

func (s *tempos) add(b float64) { s.mu.Lock(); s.got = append(s.got, b); s.mu.Unlock() }
func (s *tempos) last() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.got) == 0 {
		return 0
	}
	return s.got[len(s.got)-1]
}

func start(t *testing.T, f *fakeScript, tp *tempos) *Client {
	t.Helper()
	c := &Client{Addr: f.ln.Addr().String(), OnTempo: tp.add, MinBackoff: 10 * time.Millisecond, MaxBackoff: 40 * time.Millisecond}
	c.Start()
	t.Cleanup(c.Stop)
	return c
}

func TestFollowsTempoEvents(t *testing.T) {
	f := newFake(t, goodHello)
	tp := &tempos{}
	c := start(t, f, tp)
	conn := <-f.conns
	waitFor(t, "active", c.Active)
	conn.Write([]byte(`{"t":"event","topic":"tempo","data":{"tempo":128.5}}` + "\n"))
	waitFor(t, "tempo 128.5", func() bool { return tp.last() == 128.5 })
	conn.Write([]byte(`{"t":"event","topic":"transport","data":{"tempo":99}}` + "\n")) // wrong topic
	conn.Write([]byte(`{"t":"event","topic":"tempo","data":{"tempo":0}}` + "\n"))      // nonsense
	conn.Write([]byte(`{"t":"event","topic":"tempo","data":{"tempo":90}}` + "\n"))
	waitFor(t, "tempo 90", func() bool { return tp.last() == 90 })
	tp.mu.Lock()
	defer tp.mu.Unlock()
	for _, b := range tp.got {
		if b == 99 || b == 0 {
			t.Fatalf("took a bad event: %v", tp.got)
		}
	}
}

func TestInactiveWhenScriptHasNoTempoTopic(t *testing.T) {
	f := newFake(t, `{"ok":true,"id":1,"data":{"protocol":1,"topics":["transport"]}}`)
	c := start(t, f, &tempos{})
	time.Sleep(150 * time.Millisecond)
	if c.Active() {
		t.Fatal("active against a script without the tempo topic")
	}
}

func TestInactiveWhenProtocolTooOld(t *testing.T) {
	f := newFake(t, `{"ok":true,"id":1,"data":{"protocol":0,"topics":["tempo"]}}`)
	c := start(t, f, &tempos{})
	time.Sleep(150 * time.Millisecond)
	if c.Active() {
		t.Fatal("active with protocol 0")
	}
}

func TestInactiveAgainstAnOldBrowserScript(t *testing.T) {
	f := newFake(t, "OK") // PushHackBrowser answers a JSON line with plain OK
	c := start(t, f, &tempos{})
	time.Sleep(150 * time.Millisecond)
	if c.Active() {
		t.Fatal("active against a plain-text script")
	}
}

func TestReconnectsAfterTheScriptDrops(t *testing.T) {
	f := newFake(t, goodHello)
	tp := &tempos{}
	c := start(t, f, tp)
	conn := <-f.conns
	waitFor(t, "active", c.Active)
	conn.Close()
	waitFor(t, "inactive after the drop", func() bool { return !c.Active() })
	conn2 := <-f.conns
	waitFor(t, "active again", c.Active)
	conn2.Write([]byte(`{"t":"event","topic":"tempo","data":{"tempo":140}}` + "\n"))
	waitFor(t, "tempo after reconnect", func() bool { return tp.last() == 140 })
}

func TestKeepAliveSendsHelloOnAQuietSocket(t *testing.T) {
	f := newFake(t, goodHello)
	c := &Client{Addr: f.ln.Addr().String(), OnTempo: func(float64) {}, KeepAlive: 30 * time.Millisecond}
	c.Start()
	t.Cleanup(c.Stop)
	<-f.conns
	waitFor(t, "hellos while quiet", func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.hellos >= 3
	})
	if !c.Active() {
		t.Fatal("dropped a healthy quiet connection")
	}
}

func TestInactiveWhenNothingListens(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()
	c := &Client{Addr: addr, MinBackoff: 10 * time.Millisecond}
	c.Start()
	defer c.Stop()
	time.Sleep(60 * time.Millisecond)
	if c.Active() {
		t.Fatal("active with no script")
	}
}
