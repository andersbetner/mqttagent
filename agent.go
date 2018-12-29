package agent

import (
	"errors"
	"os"
	"os/signal"
	"time"

	"github.com/eclipse/paho.mqtt.golang"
	log "github.com/sirupsen/logrus"
)

// Agent runs an mqtt client
type Agent struct {
	client        mqtt.Client
	clientID      string
	terminated    bool
	subscriptions []subscription
}

type subscription struct {
	topic   string
	handler mqtt.MessageHandler
}

// Close agent
func (a *Agent) Close() {
	a.client.Disconnect(250)
	a.terminated = true
}

// Subscribe to topic
func (a *Agent) Subscribe(topic string, handler mqtt.MessageHandler) (err error) {
	token := a.client.Subscribe(topic, 2, handler)
	if token.WaitTimeout(2*time.Second) == false {
		return errors.New("Subscribe timout")
	}
	if token.Error() != nil {
		return token.Error()
	}
	log.WithFields(log.Fields{
		"topic": topic}).Info("Subscribe")
	a.subscriptions = append(a.subscriptions, subscription{topic, handler})

	return nil
}

// Publish things
func (a *Agent) Publish(topic string, retain bool, payload string) (err error) {
	token := a.client.Publish(topic, 2, retain, payload)
	if token.WaitTimeout(2*time.Second) == false {
		return errors.New("Publish timout")
	}
	if token.Error() != nil {
		return token.Error()
	}
	return nil
}

// NewAgent creates an agent
func NewAgent(host string, clientID string) (a *Agent) {
	a = &Agent{}
	a.clientID = clientID

	// mqtt.DEBUG = log.New(os.Stdout, "", 0)
	// mqtt.ERROR = log.New(os.Stdout, "", 0)
	opts := mqtt.NewClientOptions().AddBroker(host).SetClientID(clientID)
	opts.SetKeepAlive(5 * time.Second)
	opts.SetPingTimeout(5 * time.Second)
	opts.OnConnectionLost = func(c mqtt.Client, err error) {
		log.WithField("error", err).Info("Lost connection")
	}
	opts.OnConnect = func(c mqtt.Client) {
		log.Info("Connect")

		//Subscribe here, otherwise after connection lost,
		//you may not receive any message
		for _, s := range a.subscriptions {
			if token := c.Subscribe(s.topic, 2, s.handler); token.Wait() && token.Error() != nil {
				log.WithField("error", token.Error()).Error("Can't subscribe")
				os.Exit(1)
			}
			log.WithField("topic", s.topic).Info("Resubscribe")
		}
	}
	a.client = mqtt.NewClient(opts)

	go func() {
		done := make(chan os.Signal)
		signal.Notify(done, os.Interrupt)
		<-done
		log.Info("Shutting down agent")
		a.Close()
	}()

	return a
}

// Connect opens a new connection
func (a *Agent) Connect() (err error) {
	token := a.client.Connect()
	if token.WaitTimeout(2*time.Second) == false {
		return errors.New("Open timeout")
	}
	if token.Error() != nil {
		return token.Error()
	}

	return
}

func (a *Agent) IsTerminated() bool {
	return a.terminated
}
