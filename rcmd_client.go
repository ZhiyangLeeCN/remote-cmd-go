package rcmd


import (
	"net"
	"sync"
)

type RemoteCmdClient struct {
	protocolNet   *ProtocolNet
	connTable     map[string]net.Conn
	connTableLock sync.RWMutex
	wg            sync.WaitGroup
}

func NewRemoteCmdClient() (*RemoteCmdClient) {
	protocolNet := NewProtocolNet(true)
	remoteCmdClient := &RemoteCmdClient{
		protocolNet: protocolNet,
		connTable:  map[string]net.Conn{},
	}
	remoteCmdClient.wg.Add(1)

	return remoteCmdClient
}

func (rcc *RemoteCmdClient)SendSync(address string, protocol *Protocol, timeoutMillis int64) (*Protocol,error) {
	conn,err := rcc.GetConnOrCreate(address)
	if err != nil {
		return nil, err
	}
	return rcc.protocolNet.SendSync(conn, protocol, timeoutMillis)
}

func (rcc *RemoteCmdClient)SendAsync(address string, protocol *Protocol, callback InvokeCallback, timeoutMillis int64) error {
	conn,err := rcc.GetConnOrCreate(address)
	if err != nil {
		return err
	}
	return rcc.protocolNet.SendAsync(conn, protocol, callback, timeoutMillis)
}

func (rcc *RemoteCmdClient)SendOneWay(address string, protocol *Protocol) error {
	conn,err := rcc.GetConnOrCreate(address)
	if err != nil {
		return err
	}
	return rcc.protocolNet.SendOneWay(conn, protocol)
}

func (rcc *RemoteCmdClient)GetConnOrCreate(address string) (net.Conn, error)  {
	conn := rcc.GetConn(address)
	if conn != nil {
		return conn, nil
	}

	conn, err := rcc.CreateConn(address)
	if err != nil {
		return nil, err
	}

	return conn, err
}

func (rcc *RemoteCmdClient) GetConn(address string) (net.Conn)  {
	defer rcc.connTableLock.RUnlock()
	rcc.connTableLock.RLock()
	return rcc.connTable[address]
}

func (rcc *RemoteCmdClient) CreateConn(address string) (net.Conn, error) {
	defer rcc.connTableLock.Unlock()
	rcc.connTableLock.Lock()
	conn := rcc.connTable[address]
	if conn != nil {
		return conn, nil
	}

	conn, err := rcc.dialTcpConnAndStartReceiveLoop(address)
	if err != nil {
		return nil, err
	}
	rcc.connTable[conn.RemoteAddr().String()] = conn
	return conn, nil

}

func (rcc *RemoteCmdClient) dialTcpConnAndStartReceiveLoop(address string) (net.Conn,error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err;
	}

	go rcc.protocolNet.receiveLoop(conn)

	return conn, nil
}

func (rcc *RemoteCmdClient) ReleaseConn(addr string, conn net.Conn) {
	defer rcc.connTableLock.Unlock()
	conn.Close()
	rcc.connTableLock.Lock()
	delete(rcc.connTable, addr)
}

func (rcc *RemoteCmdClient)Start()  {
	rcc.protocolNet.Start()
	go rcc.handleProtocolNetEvent()
	
}

func (rcc *RemoteCmdClient)Stop()  {

	defer func() {
		rcc.wg.Done()
		rcc.connTableLock.Unlock()
	}()

	rcc.connTableLock.Lock()

	for _, conn := range rcc.connTable {
		conn.Close()
	}

	rcc.protocolNet.Stop()

}

func (rcc *RemoteCmdClient)handleProtocolNetEvent() {

	for event := range rcc.protocolNet.GetEvents() {

		if event.Type == ProtocolNetErrorEvent {

			rcc.ReleaseConn(
				event.Conn.RemoteAddr().String(),
				event.Conn,
			)

		}

	}
	
}

func (rcc *RemoteCmdClient)PutDefaultCmdHandler(handle CmdHandle) {
	rcc.protocolNet.setDefaultProtocolHandler(handle)
}

func (rcc *RemoteCmdClient)PutCmdHandler(cmdCode int16, handle CmdHandle) {
	rcc.protocolNet.setProtocolHandler(cmdCode, handle)
}

func (rcc *RemoteCmdClient)RemoveCmdHandler(cmdCode int16) {
	rcc.protocolNet.removeProtocolHandler(cmdCode)
}