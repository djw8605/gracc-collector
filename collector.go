package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	log "github.com/Sirupsen/logrus"
	"github.com/opensciencegrid/gracc-collector/gracc"
)

type CollectorStats struct {
	Records       uint64
	RecordErrors  uint64
	Requests      uint64
	RequestErrors uint64
}

type Event int

const (
	GOT_RECORD Event = iota
	RECORD_ERROR
	GOT_REQUEST
	REQUEST_ERROR
)

type GraccCollector struct {
	Config *CollectorConfig
	Output *AMQPOutput
	Stats  CollectorStats
	m      sync.Mutex

	Events chan Event
}

// NewCollector initializes and returns a new Gracc collector.
func NewCollector(conf *CollectorConfig) (*GraccCollector, error) {
	var g GraccCollector
	g.Config = conf

	g.Events = make(chan Event)
	go g.LogEvents()

	if o, err := InitAMQP(conf.AMQP); err != nil {
		return nil, err
	} else {
		g.Output = o
	}

	return &g, nil
}

func (g *GraccCollector) LogEvents() {
	for e := range g.Events {
		g.m.Lock()
		switch e {
		case GOT_RECORD:
			g.Stats.Records++
		case RECORD_ERROR:
			g.Stats.RecordErrors++
		case GOT_REQUEST:
			g.Stats.Requests++
		case REQUEST_ERROR:
			g.Stats.RequestErrors++
		}
		g.m.Unlock()
	}
}

func (g *GraccCollector) ServeStats(w http.ResponseWriter, r *http.Request) {
	enc := json.NewEncoder(w)
	g.m.Lock()
	stats := g.Stats
	g.m.Unlock()
	if err := enc.Encode(stats); err != nil {
		http.Error(w, "error writing stats", http.StatusInternalServerError)
	}
}

// Request is a wrapper struct for passing around an HTTP request
// and associated response writer and metadata
type Request struct {
	w     http.ResponseWriter
	r     *http.Request
	log   *log.Entry
	start time.Time
}

func (g *GraccCollector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.Events <- GOT_REQUEST
	req := &Request{
		w: w,
		r: r,
		log: log.WithFields(log.Fields{
			"address":  r.RemoteAddr,
			"length":   r.ContentLength,
			"agent":    r.UserAgent(),
			"url_path": r.URL.EscapedPath(),
		}),
		start: time.Now(),
	}
	r.ParseForm()
	if err := g.checkRequiredKeys(req, []string{"command"}); err != nil {
		g.Events <- REQUEST_ERROR
		g.handleError(req, err, http.StatusBadRequest)
		return
	}
	command := r.FormValue("command")
	switch command {
	case "update":
		g.handleUpdate(req)
	default:
		g.Events <- REQUEST_ERROR
		g.handleError(req, fmt.Errorf("unknown command"), http.StatusBadRequest)
	}
}

func (g *GraccCollector) handleUpdate(req *Request) {
	if err := g.checkRequiredKeys(req, []string{"arg1", "from"}); err != nil {
		g.Events <- REQUEST_ERROR
		g.handleError(req, err, http.StatusBadRequest)
		return
	}
	updateLogger := log.WithFields(log.Fields{
		"from": req.r.FormValue("from"),
	})
	if req.r.FormValue("arg1") == "xxx" {
		updateLogger.Info("received ping")
		g.handleSuccess(req)
		return
	} else {
		if err := g.checkRequiredKeys(req, []string{"bundlesize"}); err != nil {
			g.Events <- REQUEST_ERROR
			g.handleError(req, err, http.StatusBadRequest)
			return
		}
		bundlesize, err := strconv.Atoi(req.r.FormValue("bundlesize"))
		if err != nil {
			g.Events <- REQUEST_ERROR
			updateLogger.WithField("error", err).Error("error handling update")
			g.handleError(req, fmt.Errorf("error interpreting bundlesize"), http.StatusBadRequest)
			return
		}
		if err := g.ProcessBundle(req.r.FormValue("arg1"), bundlesize); err == nil {
			updateLogger.WithField("bundlesize", req.r.FormValue("bundlesize")).Info("received update")
			g.handleSuccess(req)
			return
		} else {
			g.Events <- REQUEST_ERROR
			updateLogger.WithField("error", err).Error("error handling update")
			g.handleError(req, fmt.Errorf("error processing bundle (%s)", err), http.StatusInternalServerError)
			return
		}
	}
}

func (g *GraccCollector) checkRequiredKeys(req *Request, keys []string) error {
	for _, k := range keys {
		if req.r.FormValue(k) == "" {
			err := fmt.Sprintf("no %v", k)
			return fmt.Errorf(err)
		}
	}
	return nil
}

func (g *GraccCollector) handleError(req *Request, err error, code int) {
	res := fmt.Sprintf("Error: %s", err)
	req.log.WithFields(log.Fields{
		"response":      res,
		"response-code": code,
		"error":         err,
		"response-time": time.Since(req.start).Nanoseconds(),
	}).Info("handled request")
	req.w.WriteHeader(code)
	fmt.Fprint(req.w, res)
}

func (g *GraccCollector) handleSuccess(req *Request) {
	req.log.WithFields(log.Fields{
		"response":      "OK",
		"response-code": 200,
		"response-time": time.Since(req.start).Nanoseconds(),
	}).Info("handled request")
	fmt.Fprintf(req.w, "OK")
}

// ScanBundle is a split function for bufio.Scanner that splits the bundle
// at each pipe/bar character "|" that does not occur in a single- or double-
// quote delimited string.
func ScanBundle(data []byte, atEOF bool) (advance int, token []byte, err error) {
	inString := false
	escape := false
	var stringDelim rune
	for width, i := 0, 0; i < len(data); i += width {
		var r rune
		r, width = utf8.DecodeRune(data[i:])
		switch r {
		case '|':
			if !inString {
				return i + width, data[0:i], nil
			}
		case '\'', '"':
			if inString && !escape && r == stringDelim {
				inString = false
			} else if !inString {
				inString = true
				stringDelim = r
			}
		}
		escape = (r == '\\' && !escape)
	}
	// If we're at EOF, we have a final, non-terminated bundle. Return it.
	if atEOF {
		return len(data), data, bufio.ErrFinalToken
	}
	// Request more data.
	return 0, nil, nil
}

// ProcessBundle parses a bundle and publishes records to AMQP broker.
func (g *GraccCollector) ProcessBundle(bundle string, bundlesize int) error {
	// setup AMQP channel
	w, err := g.Output.NewWorker(bundlesize)
	if err != nil {
		log.Error("error starting AMQP worker")
		return err
	}
	defer w.Close()

	// Parse bundle
	received := 0
	bs := bufio.NewScanner(strings.NewReader(bundle))
	bs.Buffer(make([]byte, g.Config.StartBufferSize), g.Config.MaxBufferSize)
	bs.Split(ScanBundle)
ScannerLoop:
	for bs.Scan() {
		tok := bs.Text()
		switch tok {
		case "":
			continue
		case "replication":
			parts := make(map[string]string, 3)
			for _, p := range []string{"rec", "raw", "extra"} {
				if bs.Scan() {
					parts[p] = bs.Text()
				} else {
					break ScannerLoop
				}
			}
			g.Events <- GOT_RECORD
			// publish record
			var jur gracc.JobUsageRecord
			if err := jur.ParseXML([]byte(parts["rec"])); err != nil {
				log.WithFields(log.Fields{
					"error": err,
					"rec":   parts["rec"],
					"raw":   parts["raw"],
					"extra": parts["extra"],
				}).Error("error processing record XML")
				g.Events <- RECORD_ERROR
				return fmt.Errorf("error processing replicated record")
			}
			if err := w.PublishRecord(&jur); err != nil {
				log.Error(err)
				g.Events <- RECORD_ERROR
				return fmt.Errorf("error publishing record")
			}
			received++
		}
	}
	// check for scanner errors
	if err := bs.Err(); err != nil {
		return fmt.Errorf("error parsing bundle: %s", err)
	}
	// check that we got all expected records
	if received != bundlesize {
		return fmt.Errorf("actual bundle size (%d) different than expected (%d)", received, bundlesize)
	}
	// wait for confirms that all records were received and routed
	if err := w.Wait(); err != nil {
		return err
	}
	return nil
}
