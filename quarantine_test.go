package main

import (
  "testing"
	"fmt"
	"net/url"
	"net/http/httptest"
	"bytes"
	"crypto/sha256"
  "encoding/hex"
	"path"
)



func TestQuarantine(m *testing.T) {
	
	q, err := NewQuarantine("/tmp")
	if err != nil {
		fmt.Printf("Error making quarantine: %s\n", err)
	}

	testURL := "http://" + config.Address + ":" + config.Port + "/rmi"
	v := url.Values{}
	v.Set("command", "update")
	v.Set("from", "localhost")
	v.Set("arg1", testBundle)
	v.Set("bundlesize", "1")
	
	encodedBody := v.Encode()
	
	buf := bytes.NewBuffer([]byte(encodedBody))
	
	/* 
	h := sha256.New()
	h.Write([]byte(testBundle))
	hexString := hex.EncodeToString(h.Sum(nil))
	newPath := path.Join("/tmp", hexString[:2], hexString[2:])
	*/
	
	httpreq := httptest.NewRequest("POST", testURL, buf)
	
	req := &Request{
		r: httpreq,
	}
	
	var path1 string
	path1, err = q.ProcessQuarantine(req)
	if err != nil {
		fmt.Printf("Error processing: %s", err)
		m.Fatalf("Error in ProcessQuarantine: %s", err)
	}
	
	buf = bytes.NewBuffer([]byte(encodedBody))
	httpreq = httptest.NewRequest("POST", testURL, buf)
	req = &Request{
		r: httpreq,
	}
	var path2 string
	path2, err = q.ProcessQuarantine(req)
	if err != nil {
		fmt.Printf("Error processing: %s", err)
		m.Fatalf("Error in ProcessQuarantine: %s", err)
	}

	
	fmt.Printf("Path1 is: %s, path2 is: %s", path1, path2)
	if path1 != path2 {
		m.Fatalf("Same body isn't the same path, Path1 = %s, Path2 = %s\n", path1, path2)
	}
	
}
