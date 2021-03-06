package main

import (
	"encoding/xml"
	"fmt"
	"math/rand"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/opensciencegrid/gracc-collector/gracc"
	"github.com/streadway/amqp"
)

type AMQPConfig struct {
	Host             string        `env:"HOST"`
	Port             string        `env:"PORT"`
	Scheme           string        `env:"SCHEME"`
	Vhost            string        `env:"VHOST"`
	User             string        `env:"USER"`
	Password         string        `env:"PASSWORD"`
	Format           string        `env:"FORMAT"`
	Exchange         string        `env:"EXCHANGE"`
	ExchangeType     string        `env:"EXCHANGETYPE"`
	Durable          bool          `env:"DURABLE"`
	AutoDelete       bool          `env:"AUTODELETE"`
	Internal         bool          `env:"INTERNAL"`
	RoutingKey       string        `env:"ROUTINGKEY"`
	Retry            string        `env:"RETRY"`
	RetryDuration    time.Duration `env:"-"`
	MaxRetry         string        `env:"MAXRETRY"`
	MaxRetryDuration time.Duration `env:"-"`
}

func (c *AMQPConfig) Validate() error {
	if c.Scheme == "" {
		c.Scheme = "amqp"
	}
	var err error
	c.RetryDuration, err = time.ParseDuration(c.Retry)
	if err != nil {
		return fmt.Errorf("error parsing Retry: %s", err)
	}
	c.MaxRetryDuration, err = time.ParseDuration(c.MaxRetry)
	if err != nil {
		return fmt.Errorf("error parsing MaxRetry: %s", err)
	}
	return nil
}

type AMQPOutput struct {
	Config     AMQPConfig
	URI        string
	connection *amqp.Connection
	isBlocked  bool
	m          sync.Mutex
}

func InitAMQP(conf AMQPConfig) (*AMQPOutput, error) {
	var a = &AMQPOutput{
		Config: conf,
		URI: conf.Scheme + "://" + conf.User + ":" + conf.Password + "@" +
			conf.Host + ":" + conf.Port + "/" + conf.Vhost,
	}
	if err := a.setup(); err != nil {
		return nil, err
	}
	// declare exchange
	ch, err := a.OpenChannel()
	if err != nil {
		log.Error(err)
		return nil, NewAMQPError("error opening channel")
	}
	log.WithFields(log.Fields{
		"name":       a.Config.Exchange,
		"type":       a.Config.ExchangeType,
		"durable":    a.Config.Durable,
		"autoDelete": a.Config.AutoDelete,
		"internal":   a.Config.Internal,
	}).Debug("declaring exchange")
	if err = ch.ExchangeDeclare(a.Config.Exchange,
		a.Config.ExchangeType,
		a.Config.Durable,
		a.Config.AutoDelete,
		a.Config.Internal,
		false,
		nil); err != nil {
		ch.Close()
		log.Error(err)
		return nil, NewAMQPError("error declaring exchange")
	}
	ch.Close()
	return a, nil
}

// backoff computes the next backoff duration, using "Decorrelated Jitter" method.
// https://www.awsarchitectureblog.com/2015/03/backoff.html
func backoff(last time.Duration, base time.Duration, max time.Duration) time.Duration {
	var sleep time.Duration
	sleep = base + time.Duration(rand.Int63n(int64(last)*3-int64(base)))
	if sleep > max {
		return max
	}
	return sleep
}

func (a *AMQPOutput) setup() error {
	a.m.Lock()
	defer a.m.Unlock()
	if a.connection != nil {
		a.connection.Close()
		a.connection = nil
	}
	log.WithFields(log.Fields{
		"user":  a.Config.User,
		"host":  a.Config.Host,
		"vhost": a.Config.Vhost,
		"port":  a.Config.Port,
	}).Info("AMQP: connecting to RabbitMQ")
	var err error
	connect := func() error {
		a.connection, err = amqp.Dial(a.URI)
		return err
	}
	sleep := a.Config.RetryDuration
	for err = connect(); err != nil; err = connect() {
		log.WithFields(log.Fields{
			"error": err,
			"retry": sleep.String(),
		}).Error("AMQP: error connecting to RabbitMQ")
		a.m.Unlock()
		time.Sleep(sleep)
		a.m.Lock()
		sleep = backoff(sleep, a.Config.RetryDuration, a.Config.MaxRetryDuration)
	}
	log.Info("AMQP: connection established")
	// listen for close events
	closing := a.connection.NotifyClose(make(chan *amqp.Error))
	go func() {
		for c := range closing {
			log.WithFields(log.Fields{
				"code":             c.Code,
				"reason":           c.Reason,
				"server-initiated": c.Server,
				"can-recover":      c.Recover,
			}).Warning("AMQP: connection closed")
			a.setup()
		}
	}()
	// listen for blocking events
	a.isBlocked = false
	blockings := a.connection.NotifyBlocked(make(chan amqp.Blocking))
	go func() {
		for b := range blockings {
			if b.Active {
				log.WithField("reason", b.Reason).Warning("AMQP: TCP blocked")
				a.m.Lock()
				a.isBlocked = true
				a.m.Unlock()
			} else {
				log.Info("AMQP: TCP unblocked")
				a.m.Lock()
				a.isBlocked = false
				a.m.Unlock()
			}
		}
	}()
	return nil
}

// OpenChannel locks the connection and opens a new channel.
func (a *AMQPOutput) OpenChannel() (*amqp.Channel, error) {
	a.m.Lock()
	defer a.m.Unlock()
	if a.isBlocked {
		return nil, NewAMQPError("connection is blocked by broker")
	}
	if a.connection == nil {
		return nil, NewAMQPError("connection is not open")
	}
	return a.connection.Channel()
}

// AMQPWorker manages an AMQP channel, including
// handling confirms, returns, and the channel or
// connection being closed.
type AMQPWorker struct {
	Channel  *amqp.Channel
	Config   AMQPConfig
	confirms chan amqp.Confirmation
	closing  chan *amqp.Error
	returns  chan amqp.Return
	flow     chan bool
	lastTag  uint64
}

// Initialize and return a new worker. bundleSize is the expected number
// of records this worker will handle.
func (a *AMQPOutput) NewWorker(bundleSize int) (*AMQPWorker, error) {
	ll := log.WithFields(log.Fields{
		"where": "AMQPOutput.NewWorker",
	})
	log.Debug("starting AMQP worker")
	// open channel
	ch, err := a.OpenChannel()
	if err != nil {
		ll.Error(err)
		return nil, NewAMQPError("error opening channel")
	}
	// put channel into confirm mode
	if err = ch.Confirm(false); err != nil {
		ll.Error(err)
		return nil, NewAMQPError("Channel could not be put into confirm mode")
	}
	return &AMQPWorker{
		Channel:  ch,
		Config:   a.Config,
		confirms: ch.NotifyPublish(make(chan amqp.Confirmation, bundleSize)),
		closing:  ch.NotifyClose(make(chan *amqp.Error, 1)),
		returns:  ch.NotifyReturn(make(chan amqp.Return, bundleSize)),
		flow:     ch.NotifyFlow(make(chan bool)),
	}, nil
}

// PublishRecords sends the Record to the AMQP broker. It does not
// wait for confirmation! Call Wait() to wait for confirms and returns.
func (w *AMQPWorker) PublishRecord(rec gracc.Record) error {
	ll := log.WithFields(log.Fields{
		"where": "AMQPWorker.PublishRecord",
	})
	// check for flow control
	select {
	case f := <-w.flow:
		if f {
			return NewAMQPError("under flow control")
		}
	default:
	}
	// publish record
	pub := w.makePublishing(rec)
	if pub == nil {
		return NewAMQPError("error making AMQP publishing from Record")
	}
	ll.WithFields(log.Fields{
		"exchange":   w.Config.Exchange,
		"routingKey": w.Config.RoutingKey,
		"record":     rec.Id(),
	}).Debug("publishing record")
	if err := w.Channel.Publish(
		w.Config.Exchange, // exchange
		"",                // routing key
		true,              // mandatory
		false,             // immediate
		*pub); err != nil {
		ll.Error(err)
		return NewAMQPError("error publishing to channel")
	}
	w.lastTag++
	ll.WithFields(log.Fields{
		"exchange":   w.Config.Exchange,
		"routingKey": w.Config.RoutingKey,
		"record":     rec.Id(),
		"tag":        w.lastTag,
	}).Debug("record sent")
	return nil
}

// Wait will wait for confirms for all publishings sent so far.
// It will also listen for returns, and will return an error if
// a record is returned or if timeout elapses (unless timout<=0).
func (w *AMQPWorker) Wait(timeout time.Duration) error {
	ll := log.WithFields(log.Fields{
		"where": "AMQPWorker.Wait",
	})
	if w.lastTag < 1 {
		ll.Warning("no records were sent")
		return nil
	}
	var tc <-chan time.Time
	if timeout > 0 {
		tc = time.After(timeout)
	} else {
		tc = make(<-chan time.Time)
	}
	var returns, nacks int
WaitLoop:
	for {
		select {
		case <-tc:
			ll.WithFields(log.Fields{
				"timeout": timeout.String(),
			}).Warning("timed out while waiting for confirms")
			return NewAMQPError("timed out while waiting for confirms")
		case c := <-w.closing:
			ll.WithFields(log.Fields{
				"code":             c.Code,
				"reason":           c.Reason,
				"server-initiated": c.Server,
				"can-recover":      c.Recover,
			}).Error("channel closed")
			return NewAMQPError("channel closed while waiting for confirms")
		case ret := <-w.returns:
			ll.WithFields(log.Fields{
				"code":   ret.ReplyCode,
				"reason": ret.ReplyText,
			}).Warning("record returned")
			returns++
		case confirm := <-w.confirms:
			ll.WithFields(log.Fields{
				"tag": confirm.DeliveryTag,
				"ack": confirm.Ack,
			}).Debug("confirm")
			if !confirm.Ack {
				nacks++
			}
			if confirm.DeliveryTag >= w.lastTag {
				break WaitLoop
			}
		}
	}
	if returns > 0 {
		return NewAMQPError(fmt.Sprintf("%d records were returned", returns))
	}
	if nacks > 0 {
		return NewAMQPError(fmt.Sprintf("%d records were not successfully sent", nacks))
	}
	log.Debug("all records sent successfully")
	return nil
}

// Close closes the AMQP channel and retires the worker.
// If you want to make sure all records were recieved call Wait() first!
func (w *AMQPWorker) Close() error {
	log.Debug("closing AMQP worker")
	return w.Channel.Close()
}

func (w *AMQPWorker) makePublishing(jur gracc.Record) *amqp.Publishing {
	ll := log.WithFields(log.Fields{
		"where": "AMQPWorker.makePublishing",
	})
	var pub amqp.Publishing
	switch w.Config.Format {
	case "raw":
		pub.ContentType = "text/xml"
		pub.Body = jur.Raw()
	case "xml":
		if j, err := xml.Marshal(jur); err != nil {
			ll.Error("error converting JobUsageRecord to xml")
			ll.Debugf("%v", jur)
			return nil
		} else {
			pub.ContentType = "text/xml"
			pub.Body = j
		}
	case "json":
		if j, err := jur.ToJSON("    "); err != nil {
			ll.Error("error converting JobUsageRecord to json")
			ll.Debugf("%v", jur)
			return nil
		} else {
			pub.ContentType = "application/json"
			pub.Body = j
		}
	}
	return &pub
}
