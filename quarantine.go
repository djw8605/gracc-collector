package main

import (
  "fmt"
  "time"
  "crypto/sha256"
  "encoding/hex"
  "path"
  "sync"
  "os"
	"io/ioutil"
	"io"
)


// exists returns whether the given file or directory exists or not
func exists(path string) (bool, error) {
    _, err := os.Stat(path)
    if err == nil { return true, nil }
    if os.IsNotExist(err) { return false, nil }
    return true, err
}


// Quarantine type is the structure to manage the quarentine of records
type Quarantine struct {
	outputDir string
	bytesMux sync.Mutex
	quarantineBytes int64
}

// NewQuarantine creates a new quarentine object
func NewQuarantine(outputDir string) (*Quarantine, error) {
	var q Quarantine
	q.outputDir = outputDir
	
	// Check if the output directory exists
	if exists, _ := exists(outputDir); !exists {
		// Make the quarentine directory
		if err := os.MkdirAll(outputDir, os.ModeDir); err != nil {
			return nil, err
		}
	}
	
	
	
	go q.ScanQuarantine()

	return &q, nil
}

// ScanQuarantine scans the quarentine directory to detect the current
// size in bytes and inodes
func (q* Quarantine) ScanQuarantine() {
	
	
	
	// At the end, start the MonitorQuarentine loop
	q.MonitorQuarantine()
	
}


// MonitorQuarantine periodically checks the status the quarentine
// directory to make sure it doesn't overload the node
func (q* Quarantine) MonitorQuarantine() {

	var monitorTime *time.Timer
	// Do this in a for loop
	for {
		fmt.Println("Top of monitor loop")

		// Every 30 seconds, wake up
		if monitorTime == nil {
			monitorTime = time.NewTimer(time.Second * 5)
		} else {
			monitorTime.Reset(time.Second * 5)
		}
		<-monitorTime.C

	}

}

// ProcessQuarantine processes a record and puts it into the quarentine directory
// It stores the record in a file named after the hash of the payload
func (q *Quarantine) ProcessQuarantine(req *Request) (string, error) {

	h := sha256.New()
	
	// Create a temporary file while we are processing the request
	tmpfile, err := ioutil.TempFile("", "quarantine")
	if err != nil {
		return "", err
	}

	buf := make([]byte, 1024)
	counter := 0
	
	for {
		n, err := req.r.Body.Read(buf)
		if err != nil && err != io.EOF {
    	return "", err
    }
		if n == 0 {
			break
		}
		
		// Process the chunk
		h.Write(buf)
		tmpfile.Write(buf)
		counter += n
		
	}
	
	hexString := hex.EncodeToString(h.Sum(nil))
	
	hashDir := path.Join(q.outputDir, hexString[:2])
	newPath := path.Join(q.outputDir, hexString[:2], hexString[2:])
	
	// If the record is already stored, do nothing
	if exists, _ := exists(newPath); exists {
		return newPath, nil
	}
	
	// Make the hash directory
	if err := os.MkdirAll(hashDir, os.ModeDir); err != nil {
		return newPath, err
	}
	
	// Atomic mv to the new file
	tmpfile.Close()
	os.Rename(tmpfile.Name(), newPath)
	
	q.bytesMux.Lock()
	q.quarantineBytes += int64(counter)
	q.bytesMux.Unlock()
	

	
	return newPath, nil

}

