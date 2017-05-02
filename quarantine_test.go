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
	
	h := sha256.New()
	h.Write([]byte(testBundle))
	hexString := hex.EncodeToString(h.Sum(nil))
	newPath := path.Join("/tmp", hexString[:2], hexString[2:])
	
	httpreq := httptest.NewRequest("POST", testURL, buf)
	
	req := &Request{
		r: httpreq,
	}
	
	var path string
	path, err = q.ProcessQuarantine(req)
	if err != nil {
		fmt.Printf("Error processing: %s", err)
		m.Fatalf("Error in ProcessQuarantine: %s", err)
	}
	
	fmt.Printf("Path is: %s", path)
	if path != newPath {
		m.Fatalf("Path is not the same as calculated path.  Calculated: %s, returned: %s\n", newPath, path)
	}
	
}
