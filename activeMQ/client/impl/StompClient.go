package impl

import (
	"errors"
	"fmt"
	"github.com/go-stomp/stomp"
)

type StompClient struct {
	conn          *stomp.Conn
	subscriptions map[string]*stomp.Subscription
}

func NewStompClient() *StompClient {
	return &StompClient{}
}

func (s *StompClient) Connect(url string) error {
	s.subscriptions = map[string]*stomp.Subscription{}
	var err error
	s.conn, err = stomp.Dial("tcp", url)
	return err
}

func (s *StompClient) SubscribeToQueue(queueName string, messageChanel chan []byte) error {
	if s.conn != nil {
		sub, err := s.conn.Subscribe(queueName, stomp.AckAuto)
		if err == nil {
			if s.subscriptions == nil {
				s.subscriptions = map[string]*stomp.Subscription{}
			}
			s.subscriptions[queueName] = sub
			go func(subscription *stomp.Subscription, c chan []byte) {
				for {
					val := <-subscription.C
					if val != nil {
						c <- val.Body
					} else {
						fmt.Println("Subscription timed out, renewing...")
						_ = s.SubscribeToQueue(queueName, c)
						break
					}
				}
			}(sub, messageChanel)
			return nil
		}
		return err
	}
	return errors.New("client was nil")
}

func (s *StompClient) Unsubscribe(queueName string) error {
	if s.subscriptions != nil {
		return s.subscriptions[queueName].Unsubscribe()
	}
	return errors.New("no subscriptions available")
}

func (s *StompClient) Disconnect() error {
	if s.conn != nil {
		return s.conn.Disconnect()
	}
	return errors.New("client was nil")
}

func (s *StompClient) SendMessageToQueue(queueName, contentType string, body []byte) error {
	if s.conn != nil {
		return s.conn.Send(queueName, contentType, body)
	}
	return errors.New("client was nil")
}
