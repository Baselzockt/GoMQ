package impl

import (
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
	if s.conn == nil {
		return nil
	}

	for _, subscription := range s.subscriptions {
		err := subscription.Unsubscribe()
		if err != nil {
			return err
		}
	}

	err := s.conn.Disconnect()
	s.conn = nil
	return err
}

func (s *StompClient) reconnect() error {
	s.Disconnect()
	return s.Connect()
}

func (s *StompClient) SubscribeToQueue(queueName string, messageChanel *chan []byte) error {
	log.Debug("Trying to subscribe to: " + queueName)
	if s.conn != nil {
		return nil
	}

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

func handleSubscription(subscription *stomp.Subscription, c *chan []byte, s *StompClient, queueName string) {
	for {
		log.Debug("Trying to convert")
		val := <-subscription.C

		if val == nil {
			log.Error("Subscription timed out, renewing...")
			err := s.renewSubscription(queueName, c)

			if err != nil {
				log.Error("Could not create subscription. Trying to unsubscribe")
				s.Unsubscribe(queueName)
				close(*c)
				return
			}
			log.Debug("Created new subscription")
			break
		}
		*c <- val.Body
		log.Debug("Conversion successful")
	}
	close(*c)
}

func (s StompClient) renewSubscription(queueName string, channel *chan []byte) error {
	err := s.Connect()
	if err != nil {
		return err
	}
	return s.SubscribeToQueue(queueName, channel)
}

func (s *StompClient) Unsubscribe(queueName string) error {
	log.Debug("Trying to unsubscribe from queue: " + queueName)
	if s.subscriptions == nil {
		return nil
	}

	if s.subscriptions[queueName] == nil {
		return nil
	}

	log.Debug("Unsubscribing")
	return s.subscriptions[queueName].Unsubscribe()
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
