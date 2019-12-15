package main

import (
	"log"
	"net"
	"runtime"
	"sync"
	"time"
)

// Bedrock parameters needed to run
type Bedrock struct {
	// inbound connection listening for clients
	listenConn *net.UDPConn

	// address of destination server
	server *net.UDPAddr

	// lock for maps
	mux sync.Mutex

	// map from client address to server connection
	clientMap map[string]*net.UDPConn

	// timeout for connection to server
	timeout time.Duration
}

const connectionTimeoutSeconds int = 5

type udpMessage struct {
	addr   net.Addr
	packet []byte
}

func handlePacket(fromClient bool, packet []byte) {
	var starter string
	if fromClient {
		starter = "c"
	} else {
		starter = "s"
	}
	log.Printf("%s %02X\n", starter, packet[0])
}

func (b *Bedrock) serverPacketWorker(serverConn *net.UDPConn, clientAddr net.Addr) {
	log.Printf("server worker started for %s\n", clientAddr.String())

	for {
		buf := make([]byte, 1024)
		serverConn.SetReadDeadline(time.Now().Add(b.timeout))
		n, err := serverConn.Read(buf)
		if err != nil {
			log.Printf("Error or timeout reading from server, closing for %s\n", clientAddr.String())
			b.mux.Lock()
			serverConn.Close()
			delete(b.clientMap, clientAddr.String())
			b.mux.Unlock()
			return
		}
		b.listenConn.WriteTo(buf[:n], clientAddr)
		handlePacket(false, buf[:n])
	}
}

func (b *Bedrock) clientPacketWorker(messages <-chan udpMessage) {
	log.Println("Client worker started")
	for m := range messages {
		// find the server
		var ok bool
		var server *net.UDPConn
		addrString := m.addr.String()
		b.mux.Lock()
		if server, ok = b.clientMap[addrString]; ok { // if we have the server

		} else { // if we don't have the server saved
			b.mux.Unlock() // unlock while connecting
			var err error
			server, err = net.DialUDP("udp", nil, b.server)
			if err != nil {
				log.Printf("Could not connect to server, discarding message from %s\n", m.addr.String())
				continue
			}

			b.mux.Lock()
			if s, ok := b.clientMap[addrString]; ok { // if another process opened first
				server.Close() // close the new one we made
				server = s     // get the mapped version
			} else { // if we're the first
				b.clientMap[addrString] = server        // save the server connection
				go b.serverPacketWorker(server, m.addr) // start up server listener
			}
		}
		b.mux.Unlock()

		// write to the server
		handlePacket(true, m.packet)
		server.Write(m.packet)
	}
}

// Start the proxy
func (b *Bedrock) Start(listenPort int, serverAddress string, timeout time.Duration) {
	// make channels
	clientMessages := make(chan udpMessage, 100)

	// make maps
	b.clientMap = make(map[string]*net.UDPConn)

	sA, err := net.ResolveUDPAddr("udp", serverAddress)
	if err != nil {
		log.Fatalln("Could not resolve server address")
		return
	}

	// get the port ready
	listenAddress := net.UDPAddr{Port: listenPort}
	conn, err := net.ListenUDP("udp", &listenAddress)
	if err != nil {
		log.Fatalln("Error opening listening address")
		return
	}
	defer conn.Close()
	b.listenConn = conn
	b.server = sA
	b.timeout = timeout

	// start client packet workers
	for i := 1; i < runtime.NumCPU()*4; i++ {
		go b.clientPacketWorker(clientMessages)
	}

	for {
		buf := make([]byte, 1024)
		n, addr, err := b.listenConn.ReadFrom(buf) // wait for message from client
		if err != nil {
			log.Printf("Error reading from client %s\n", addr.String())
			continue
		}
		msg := udpMessage{addr: addr, packet: buf[:n]}
		clientMessages <- msg // send message to workers
	}
}

func main() {
	bedrock := Bedrock{}

	bedrock.Start(19132, "mco.mineplex.com:19132", 5*time.Second)
}
