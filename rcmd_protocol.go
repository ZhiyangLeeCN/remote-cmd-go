package rcmd

import (
	"bytes"
	"encoding/binary"
	"sync/atomic"
	"encoding/json"
)

const (
	//版本号
	Version = 1

	EmptyFlag = 0x0

	//标识该协议是请求
	RequestFlag = 0x1
	//标识该协议是响应
	ResponseFlag = 0x2
	//标识该请求是OneWay无需响应
	OneWayFlag = 0x4

	//协议总消息大小字段占用大小
	ProtocolSizeFieldLen = 4
	//协议标志位字段占用大小
	ProtocolFlagFieldLen = 4
	//协议版本字段占用大小
	ProtocolVersionFieldLen = 2
	//协议命令代码字段占用大小
	ProtocolCmdCodeFieldLen = 2
	//协议请求标识ID字段占用大小
	ProtocolSeqFieldLen = 4
)

var seq int32

type ProtocolHeader struct {
	Flag     int32
	Version  int16
	CmdCode  int16
	Seq      int32
}

type Protocol struct {
	ProtocolHeader *ProtocolHeader
	Body           []byte
}

func NewCmdRequest(cmdCode int16, body interface{}) *Protocol {

	protocolHeader := NewProtocolHeader(
		RequestFlag,
		Version,
		cmdCode,
		atomic.AddInt32(&seq, 1),
	)

	bodyBuf,_ := json.Marshal(body)
	return NewProtocol(protocolHeader, bodyBuf)
}

func NewCmdResponse(cmdCode int16, seq int32, body interface{}) *Protocol  {

	protocolHeader := NewProtocolHeader(
		ResponseFlag,
		Version,
		cmdCode,
		seq,
	)

	bodyBuf,_ := json.Marshal(body)
	return NewProtocol(protocolHeader, bodyBuf)
}

func NewProtocolHeader(flag int32, version int16, cmdCode int16, seq int32) *ProtocolHeader  {
	return &ProtocolHeader{
		Flag:flag,
		Version:version,
		CmdCode:cmdCode,
		Seq:seq,
	}
}

func (ph *ProtocolHeader) ToBytes() []byte {
	buf := bytes.NewBuffer([]byte{})

	binary.Write(buf, binary.BigEndian, ph.Flag)
	binary.Write(buf, binary.BigEndian, ph.CmdCode)
	binary.Write(buf, binary.BigEndian, int16(Version))
	binary.Write(buf, binary.BigEndian, ph.Seq)

	return buf.Bytes()
}

func (ph *ProtocolHeader) SetFlag(flag int32)  {
	ph.Flag |= flag
}

func (ph *ProtocolHeader) hasFlag(flag int32) bool  {
	return (ph.Flag & flag) == flag
}

func (ph *ProtocolHeader) IsRequest() bool  {
	return ph.hasFlag(RequestFlag)
}

func (ph *ProtocolHeader) IsResponse() bool {
	return ph.hasFlag(ResponseFlag)
}

func (ph *ProtocolHeader) IsOneWay() bool  {
	return ph.hasFlag(OneWayFlag)
}

func NewProtocol(protocolHeader *ProtocolHeader, body []byte) *Protocol  {
	return &Protocol{
		ProtocolHeader:protocolHeader,
		Body: body,
	}
}

func (p *Protocol) HeaderToBytes() []byte  {
	return p.ProtocolHeader.ToBytes()
}

func (p *Protocol) BodyBytesTo(v interface{}) error  {
	return json.Unmarshal(p.Body, v)
}

func (p *Protocol) ToBytes() ([]byte, error)  {
	//首部4字节记录这次请求内容一共多少字节
	size := 4

	//记录协议头部字节大小
	headerBytes := p.HeaderToBytes()
	size += len(headerBytes)

	//如果协议体内容有设置则加上请求体大小
	if p.Body != nil {
		size += len(p.Body)
	}

	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, int32(size))
	buf.Write(headerBytes)
	if p.Body != nil {
		buf.Write(p.Body)
	}

	return buf.Bytes(), nil
}