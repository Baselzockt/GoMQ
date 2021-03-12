package impl

import (
	"errors"
	"github.com/go-stomp/stomp"
	log "github.com/sirupsen/logrus"
	"net"
	"time"
)

type StompClient struct {
	conn          *stomp.Conn
	subscriptions map[string]*stomp.Subscription
}

func NewStompClient() *StompClient {
	return &StompClient{}
}

func (s *StompClient) Connect(url string) error {
	netConn, err0 := net.DialTimeout("tcp", url, 10*time.Hour)
	if err0 != nil {
		return err0
	}

	log.Debug("Setting up subscription map")
	s.subscriptions = map[string]*stomp.Subscription{}
	var err error
	log.Debug("trying to connect to: " + url)
	s.conn, err = stomp.Connect(netConn)
	if err == nil {
		log.Debug("Successfully connected")
	}
	return err
}

func (s *StompClient) SubscribeToQueue(queueName string, messageChanel *chan []byte) error {
	log.Debug("Trying to subscribe to: " + queueName)
	if s.conn != nil {
		sub, err := s.conn.Subscribe(queueName, stomp.AckAuto)
		if err == nil {
			log.Debug("Subscription was successful, adding it to list")
			s.subscriptions[queueName] = sub
			log.Debug("Starting go function to convert from a stomp specific channel to chan []byte ")
			go func(subscription *stomp.Subscription, c *chan []byte, s *StompClient) {
				tmp := *c
				tmp <- []byte("FLUSH")
				for {
					log.Debug("Trying to convert")
					val := <-subscription.C
					if val != nil {
						tmp <- val.Body
					} else {
						log.Error("Subscription timed out, renewing...")
						err = s.SubscribeToQueue(queueName, c)
						if err != nil {
							log.Fatal(err)
						}
						log.Debug("Created new subscription")
						break
					}
					log.Debug("Conversion successful")
				}
			}(sub, messageChanel, s)
			log.Debug("Function started")
			return nil
		}
		return err
	}
	log.Error("Client was nil")
	return errors.New("client was nil")
}

func (s *StompClient) Unsubscribe(queueName string) error {
	log.Debug("Trying to unsubscribe from queue: " + queueName)
	if s.subscriptions != nil {
		log.Debug("Unsubscribing")
		return s.subscriptions[queueName].Unsubscribe()
	}
	log.Error("no subscriptions available")
	return errors.New("no subscriptions available")
}

func (s *StompClient) Disconnect() error {
	log.Debug("Trying to disconnect")
	if s.conn != nil {
		return s.conn.Disconnect()
	}
	log.Error("Could not disconnect since there was no connection")
	return errors.New("client was nil")
}

func (s *StompClient) SendMessageToQueue(queueName, contentType string, body []byte) error {
	log.Debug("Trying to send message to Queue")
	if s.conn != nil {
		return s.conn.Send(queueName, contentType, body)
	}
	return errors.New("client was nil")
}
