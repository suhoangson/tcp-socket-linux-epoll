package main

import (
	"flag"
	//"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"syscall"
	"time"

	"github.com/libp2p/go-reuseport"
	"github.com/rcrowley/go-metrics"
	"reflect"
	"sync"
	"golang.org/x/sys/unix"
	"fmt"
	"sync/atomic"
)

var (
	c = flag.Int("c", 8, "concurrency")
	read_latency int64 
	write_latency int64
	total_request int64
)

var (
	opsRate = metrics.NewRegisteredMeter("ops", nil)
)

type epoll struct {
	fd          int
	connections map[int]net.Conn
	lock        *sync.RWMutex
}

func MkEpoll() (*epoll, error) {
	fd, err := unix.EpollCreate1(0)
	if err != nil {
		return nil, err
	}
	return &epoll{
		fd:          fd,
		lock:        &sync.RWMutex{},
		connections: make(map[int]net.Conn),
	}, nil
}

func (e *epoll) Add(conn net.Conn) error {
	// Extract file descriptor associated with the connection
	fd := socketFD(conn)
	err := unix.EpollCtl(e.fd, syscall.EPOLL_CTL_ADD, fd, &unix.EpollEvent{Events: unix.POLLIN | unix.POLLHUP, Fd: int32(fd)})
	if err != nil {
		return err
	}
	e.lock.Lock()
	defer e.lock.Unlock()
	e.connections[fd] = conn
	if len(e.connections)%100 == 0 {
		log.Printf("total number of connections: %v", len(e.connections))
	}
	return nil
}

func (e *epoll) Remove(conn net.Conn) error {
	fd := socketFD(conn)
	err := unix.EpollCtl(e.fd, syscall.EPOLL_CTL_DEL, fd, nil)
	if err != nil {
		return err
	}
	e.lock.Lock()
	defer e.lock.Unlock()
	delete(e.connections, fd)
	if len(e.connections)%100 == 0 {
		log.Printf("total number of connections: %v", len(e.connections))
	}
	return nil
}

func (e *epoll) Wait() ([]net.Conn, error) {
	events := make([]unix.EpollEvent, 100)
retry:
	n, err := unix.EpollWait(e.fd, events, 100)
	if err != nil {
		if err == unix.EINTR {
			goto retry
		}
		return nil, err
	}
	e.lock.RLock()
	defer e.lock.RUnlock()
	var connections []net.Conn
	for i := 0; i < n; i++ {
		conn := e.connections[int(events[i].Fd)]
		connections = append(connections, conn)
	}
	return connections, nil
}

func socketFD(conn net.Conn) int {
	tcpConn := reflect.Indirect(reflect.ValueOf(conn)).FieldByName("conn")
	fdVal := tcpConn.FieldByName("fd")
	pfdVal := reflect.Indirect(fdVal).FieldByName("pfd")
	return int(pfdVal.FieldByName("Sysfd").Int())
}

func main() {
	read_latency = 0
	write_latency = 0
	total_request = 0
	flag.Parse()

	setLimit()
	go metrics.Log(metrics.DefaultRegistry, 5*time.Second, log.New(os.Stderr, "metrics: ", log.Lmicroseconds))

	go func() {
		if err := http.ListenAndServe(":6060", nil); err != nil {
			log.Fatalf("pprof failed: %v", err)
		}
	}()

	for i := 0; i < *c; i++ {
		go startEpoll()
	}
	for {
		time.Sleep(time.Second)
		number_request:=total_request
		if number_request==0{
			number_request=1
		}
		r_latency:=float64(read_latency)/float64(number_request)
		w_latency:=float64(write_latency)/float64(number_request)
		atomic.SwapInt64(&read_latency, 0)
		atomic.SwapInt64(&write_latency, 0)
		fmt.Printf("Total request/s: %v; Read latency: %.2f ms; Write latency: %.2f ms; \n", atomic.SwapInt64(&total_request, 0),  r_latency, w_latency)
	}
	select {}
}

func startEpoll() {
	ln, err := reuseport.Listen("tcp", ":8888")
	if err != nil {
		panic(err)
	}

	epoller, err := MkEpoll()
	if err != nil {
		panic(err)
	}

	go start(epoller)

	for {
		conn, e := ln.Accept()
		if e != nil {
			if ne, ok := e.(net.Error); ok && ne.Temporary() {
				log.Printf("accept temp err: %v", ne)
				continue
			}

			log.Printf("accept err: %v", e)
			return
		}

		if err := epoller.Add(conn); err != nil {
			log.Printf("failed to add connection %v", err)
			conn.Close()
		}
	}
}

func start(epoller *epoll) {
	var buf = make([]byte, 1024*8)
	for {
		connections, err := epoller.Wait()
		if err != nil {
			log.Printf("failed to epoll wait %v", err)
			continue
		}
		for _, conn := range connections {
			if conn == nil {
				break
			}
			//
			start_read := time.Now()
			n, err := conn.Read(buf);
			end_read := time.Now()
			if  err != nil {
				if err := epoller.Remove(conn); err != nil {
					log.Printf("failed to remove %v", err)
				}
				conn.Close()
			}
			//
			start_write := time.Now()
			_, err2 := conn.Write(buf[:n]);
			end_write := time.Now()
			if  err2 != nil {
				if err2 := epoller.Remove(conn); err2 != nil {
					log.Printf("failed to remove %v", err2)
				}
				conn.Close()
			}
			//	
			diff_read:=end_read.Sub(start_read).Milliseconds()
			diff_write:=end_write.Sub(start_write).Milliseconds()
			atomic.AddInt64(&read_latency,diff_read)
			atomic.AddInt64(&write_latency,diff_write)
			atomic.AddInt64(&total_request,1)
			opsRate.Mark(1)
		}
	}
}

func setLimit() {
	var rLimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
		panic(err)
	}
	rLimit.Cur = rLimit.Max
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
		panic(err)
	}

	log.Printf("set cur limit: %d", rLimit.Cur)
}
