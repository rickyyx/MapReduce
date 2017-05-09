package mapreduce

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"runtime"
)

var RPCProtocol string
var MasterAddress string
var WorkerAddressTemplate string

const (
	MasterSocketAddress         = DataSocketDir + "mr.master"
	WorkerSocketAddressTemplate = DataSocketDir + "mr.worker-%v"
	MasterTCPAddress            = "localhost:7782"
	WorkerTCPAddressTemplate    = "localhost:%d"
)

func init() {
	if runtime.GOOS == "windows" {
		RPCProtocol = "tcp"
		MasterAddress = MasterTCPAddress
		WorkerAddressTemplate = WorkerTCPAddressTemplate
	} else {
		RPCProtocol = "unix"
		MasterAddress = MasterSocketAddress
		WorkerAddressTemplate = WorkerSocketAddressTemplate
	}
}

func startRPCServer(address string, rcvr interface{}) (*rpc.Server, net.Listener) {
	rpcServer := rpc.NewServer()
	rpcServer.Register(rcvr)

	if RPCProtocol == "unix" {
		os.Remove(address)
	}

	l, e := net.Listen(RPCProtocol, address)
	if e != nil {
		log.Fatalf("Listen error on %s: %v\n", address, e)
	}

	return rpcServer, l
}

func serverLoop(server *rpc.Server, l net.Listener, isActive func() bool) {
	for isActive() {
		conn, err := l.Accept()
		if err != nil {
			continue
		}

		go func() {
			server.ServeConn(conn)
			conn.Close()
		}()
	}

	fmt.Println("RPC server is done.")
}

func startMasterRPCServer(mr *ParallelMaster) net.Listener {
	server, listener := startRPCServer(MasterAddress, &RPCMaster{mr})
	go func() {
		serverLoop(server, listener, mr.IsActive)
		mr.done <- true
	}()

	return listener
}

func startWorkerRPCServer(w *Worker) net.Listener {
	server, listener := startRPCServer(w.address, (*RPCWorker)(w))
	go func() {
		serverLoop(server, listener, w.IsActive)
		w.done <- true
	}()

	return listener
}

func call(address, name string, args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial(RPCProtocol, address)
	if errx != nil {
		return false
	}

	defer c.Close()
	err := c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// Invokes the task named `name` on the worker at address `worker` with the
// arguments `args`. The workers response, if any, is written to `reply`. Blocks
// until the worker responds or the connection fails. Returns `true` if the
// worker responded and `false` if the connection failed.
func callWorker(worker, name string, args interface{}, reply interface{}) bool {
	return call(worker, "RPCWorker."+name, args, reply)
}

func callMaster(name string, args interface{}, reply interface{}) bool {
	return call(MasterAddress, "RPCMaster."+name, args, reply)
}

func tcpPortIsAvailable(port int) bool {
	conn, err := net.Listen("tcp", fmt.Sprintf(WorkerAddressTemplate, port))
	conn.Close()

	return err == nil
}

func genWorkerUnixAddress() string {
	address := fmt.Sprintf(WorkerAddressTemplate, rand.Int63())
	for _, err := os.Stat(address); err == nil; _, err = os.Stat(address) {
		address = fmt.Sprintf(WorkerAddressTemplate, rand.Int63())
	}

	return address
}

func genWorkerTCPAddress() string {
	// 7783 = 7782 (MR Master) + 1; 57752 = 65535 (max) - 7783
	startPort, nPorts := int32(7783), int32(57752)

	// TODO: Ideally, we'd like to 1) use ports in a contiguous range, or 2) be
	// able to pass in a worker number and generate a port from there. 1) won't
	// work with the current setup because we would check that a port is
	// available here but then actually listen on it later. So, multiple workers
	// can be assigned the same port on the same machine.
	port := rand.Int31n(nPorts) + startPort
	for !tcpPortIsAvailable(int(port)) {
		port = rand.Int31n(nPorts) + startPort
	}

	return fmt.Sprintf(WorkerAddressTemplate, port)
}

func genWorkerAddress() string {
	if RPCProtocol == "unix" {
		return genWorkerUnixAddress()
	} else {
		return genWorkerTCPAddress()
	}
}
