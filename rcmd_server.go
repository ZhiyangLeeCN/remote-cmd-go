package rcmd

import (
	"net"
	"github.com/golang/glog"
	"sync"
)

type RemoteCmdServer struct {
	listener    net.Listener
	protocolNet *ProtocolNet
	done        chan struct{}
	wg          sync.WaitGroup
}

func NewRemoteCmdServer(address string) (*RemoteCmdServer, error) {
	listen, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	protocolNet := NewProtocolNet(false)

	remoteCmdServer := &RemoteCmdServer{
		listener: listen,
		protocolNet: protocolNet,
		done: make(chan struct{}),
	}
	remoteCmdServer.wg.Add(1)

	return remoteCmdServer, nil
}

func (rcs *RemoteCmdServer)SendSync(conn net.Conn, protocol *Protocol, timeoutMillis int64) (*Protocol,error) {
	return rcs.protocolNet.SendSync(conn, protocol, timeoutMillis)
}

func (rcs *RemoteCmdServer)SendAsync(conn net.Conn, protocol *Protocol, callback InvokeCallback, timeoutMillis int64) error {
	return rcs.protocolNet.SendAsync(conn, protocol, callback, timeoutMillis)
}

func (rcs *RemoteCmdServer)SendOneWay(conn net.Conn, protocol *Protocol) error {
	return rcs.protocolNet.SendOneWay(conn, protocol)
}

func (rcs *RemoteCmdServer)Start() {

	rcs.protocolNet.Start()

	go func() {

		defer rcs.wg.Done()

		for {
			conn, err := rcs.listener.Accept()
			if err != nil {
				select {
					case <-rcs.done:
						return
					default:
						glog.Warning(err)
						continue
				}
			}

			go rcs.protocolNet.ReceiveLoop(conn)
		}

	}()

}

func (rcs *RemoteCmdServer)Stop() {
	rcs.protocolNet.Stop()
	close(rcs.done)
	rcs.listener.Close()
	rcs.wg.Wait()
}

func (rcs *RemoteCmdServer)PutDefaultCmdHandler(handle CmdHandle) {
	rcs.protocolNet.setDefaultProtocolHandler(handle)
}

func (rcs *RemoteCmdServer)PutCmdHandler(cmdCode int16, handle CmdHandle) {
	rcs.protocolNet.setProtocolHandler(cmdCode, handle)
}

func (rcs *RemoteCmdServer)RemoveCmdHandler(cmdCode int16) {
	rcs.protocolNet.removeProtocolHandler(cmdCode)
}