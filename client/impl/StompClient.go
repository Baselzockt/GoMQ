package impl

import (
	"errors"
	"github.com/go-stomp/stomp"
	log "github.com/sirupsen/logrus"
)

type StompClient struct {
	conn          *stomp.Conn
	subscriptions map[string]*stomp.Subscription
	url           string
}

func NewStompClient() *StompClient {
	return &StompClient{}
}

func (s *StompClient) Connect(url string) error {
	s.url = url
	log.Debug("Setting up subscription map")
	s.subscriptions = map[string]*stomp.Subscription{}
	if s.conn == nil {
		var err error
		log.Debug("trying to connect to: " + url)
		s.conn, err = stomp.Dial("tcp", url)
		if err == nil {
			log.Debug("Successfully connected")
		}
		return err
	}
	return nil
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
				for {
					log.Debug("Trying to convert")
					val := <-subscription.C
					if val != nil {
						tmp <- val.Body
					} else {
						log.Error("Subscription timed out, renewing...")
						s.conn.Disconnect()
						s.conn, _ = stomp.Dial("tcp", s.url)
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
		if s.subscriptions[queueName] != nil {
			return s.subscriptions[queueName].Unsubscribe()
		}
		return errors.New("not subscribed to: " + queueName)
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
		err := s.conn.Send(queueName, contentType, body)
		if err == stomp.ErrAlreadyClosed {
			log.Debug("ActiveMQ Connection is in closed state. Reconnecting ...")
			s.conn, _ = stomp.Dial("tcp", s.url)
			return s.conn.Send(queueName, contentType, body)
		}
		return err
	}
	return errors.New("client was nil")
}
