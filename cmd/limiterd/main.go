package main

import (
	"io"
	"log"
	"net"
	"time"

	"github.com/jamiealquiza/limiter"
)

func main() {
	rl := limiter.NewLimiter(&limiter.Config{
		HardLimit: 3,
		SoftLimit: 3,
		GcInt:     5})

	server, err := net.Listen("tcp", "localhost:8080")
	if err != nil {
		log.Fatalf("Listener error: %s\n", err)
	}
	defer server.Close()

	for {
		conn, err := server.Accept()
		if err != nil {
			log.Printf("Connection handler error: %s\n", err)
			time.Sleep(1 * time.Second)
			continue
		}
		go connectionHandler(conn, rl)
	}
}

func connectionHandler(c net.Conn, rl *limiter.Limiter) {
	defer c.Close()

	b := make([]byte, 1)

	for {
		_, err := io.ReadFull(c, b)
		if err != nil {
			break
		}

	}

	switch s := rl.Incr(); {
	case s == 2:
		d := 250 * time.Millisecond
		log.Printf("Hard limit hit from %s, sleeping client for %s\n",
			c.RemoteAddr().String(), d)
		time.Sleep(d)
	}
}
