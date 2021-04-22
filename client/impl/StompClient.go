package impl

import (
	"errors"
	"github.com/go-stomp/stomp"
	log "github.com/sirupsen/logrus"
)

type StompClient struct {
	conn          *stomp.Conn
	subscriptions map[string]*stomp.Subscription
	Url           string
}

func (s *StompClient) Connect() error {
	if s.conn != nil {
		return nil
	}

	var err error
	log.Debug("trying to connect to: " + s.Url)
	s.conn, err = stomp.Dial("tcp", s.Url)
	return err
}

func (s *StompClient) Disconnect() error {
	log.Debug("Trying to disconnect")
	if s.conn != nil {
		err := s.conn.Disconnect()
		s.conn = nil
		return err
	}
	return nil
}

func (s *StompClient) reconnect() error {
	s.Disconnect()
	return s.Connect()
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
						s.conn, _ = stomp.Dial("tcp", s.Url)
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

func (s *StompClient) SendMessageToQueue(queueName, contentType string, body []byte) error {
	err := s.sendMessageToQueue(queueName, contentType, body)

	switch err {
	case nil:
		return nil
	case stomp.ErrAlreadyClosed:
		err := s.reconnect()
		if err != nil {
			return err
		}
		return s.sendMessageToQueue(queueName, contentType, body)
	default:
		return err
	}
}

func (s *StompClient) sendMessageToQueue(queueName, contentType string, body []byte) error {
	log.Debug("Trying to send message to Queue")
	err := s.Connect()
	if err != nil {
		return err
	}

	return s.conn.Send(queueName, contentType, body)
}
