package examples

import (
	"testing"
	"github.com/zhiyangleecn/remote-cmd-go"
	"fmt"
)

func TestClient(t *testing.T) {

	done := make(chan bool)

	client := rcmd.NewRemoteCmdClient()
	client.Start()

	for i := 0; i < 10000; i++ {
		rm := &Message{
			Content: "this is client send\n",
		}
		p := rcmd.NewCmdRequest(RequestCmdEcho, rm)
		r,err := client.SendSync("127.0.0.1:7887", p, 5000)
		if err != nil {
			fmt.Printf("send error.\n")
		} else {
			rpm := &Message{}
			r.BodyBytesTo(rpm)
			fmt.Printf("response:%s", rpm.Content)
		}
	}

	<-done

	client.Stop()

}
