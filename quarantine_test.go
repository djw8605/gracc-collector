package main

import (
  "testing"
	"fmt"
	"net/url"
	"net/http/httptest"
	"bytes"
)



func TestQuarantine(m *testing.T) {

	var q *Quarantine
	
	if _, err := NewQuarantine("/tmp"); err != nil {
		fmt.Printf("Error making quarentine: %s\n", err)
	}
	

	testURL := "http://" + config.Address + ":" + config.Port + "/rmi"
	v := url.Values{}
	v.Set("command", "update")
	v.Set("from", "localhost")
	v.Set("arg1", testBundle)
	v.Set("bundlesize", "1")
	
	encodedBody := v.Encode()
	
	buf := bytes.NewBuffer([]byte(encodedBody))
	
	httpreq := httptest.NewRequest("POST", testURL, buf)
	
	req := &Request{
		r: httpreq,
	}
	
	q.ProcessQuarantine(req)
	
}
