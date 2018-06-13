package examples

import (
	"testing"
	"github.com/zhiyangleecn/remote-cmd-go"
	"net"
)

func TestServer(t *testing.T) {

	done := make(chan bool)

	server,err := rcmd.NewRemoteCmdServer(":7887")
	if err != nil {

		t.Errorf("NewRemoteCmdServer error.")

	} else {

		server.PutCmdHandler(RequestCmdEcho, func(conn net.Conn, protocol *rcmd.Protocol) (interface{}, error) {

			rm := &Message{}
			protocol.BodyBytesTo(rm)

			return rm, nil

		})

		server.PutDefaultCmdHandler(func(conn net.Conn, protocol *rcmd.Protocol) (interface{}, error) {
			rm := &Message{
				Content: "this default cmd handler response.",
			}

			return rm, nil
		})

		server.Start()

		<-done

		server.Stop()

	}

}