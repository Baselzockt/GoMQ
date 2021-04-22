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
		if err != nil {
			return err
		}

		log.Debug("Subscription was successful, adding it to list")
		s.subscriptions[queueName] = sub
		log.Debug("Starting go function to convert from a stomp specific channel to chan []byte ")
		go handleSubscription(sub, messageChanel, s, queueName)
		log.Debug("Function started")
		return nil

	}
	log.Error("Client was nil")
	return errors.New("client was nil")
}

func handleSubscription(subscription *stomp.Subscription, c *chan []byte, s *StompClient, queueName string) error {
	for {
		log.Debug("Trying to convert")
		val := <-subscription.C

		if val == nil {
			log.Error("Subscription timed out, renewing...")
			err := s.reconnect()
			if err != nil {
				return err
			}

			err = s.SubscribeToQueue(queueName, c)

			if err != nil {
				log.Error("Could not create subscription")
				return err
			}

			log.Debug("Created new subscription")
			break
		}

		*c <- val.Body

		log.Debug("Conversion successful")
	}

	close(*c)
	return nil
}

func (s *StompClient) Unsubscribe(queueName string) error {
	log.Debug("Trying to unsubscribe from queue: " + queueName)
	if s.subscriptions != nil {
		log.Debug("Unsubscribing")
		if s.subscriptions[queueName] == nil {
			return errors.New("not subscribed to: " + queueName)
		}
		return s.subscriptions[queueName].Unsubscribe()
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
