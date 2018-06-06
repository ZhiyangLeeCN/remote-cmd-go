package rcmd

import "encoding/json"

type JsonEncoder struct {}

func (je *JsonEncoder) Encode(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (je *JsonEncoder) Decode(data []byte, v interface{}) error  {
	return json.Unmarshal(data, v)
}
