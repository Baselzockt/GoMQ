package impl

import (
	"errors"
	"github.com/go-stomp/stomp/testutil"
)

type StompMockClient struct {
	Conn           *testutil.FakeConn
	Url            string
	calls          []string
	messages       [][]byte
	messageChanel  map[string]*chan []byte
	closingChannel chan bool
}

func (s *StompMockClient) Connect() error {
	if s.Conn.RemoteAddr().String() != s.Url {
		return errors.New("could not connect")
	}
	s.calls = append(s.calls, "Connect to: "+s.Url)
	s.messageChanel = map[string]*chan []byte{}
	s.closingChannel = make(chan bool)
	go func() {
		for {
			select {
			case <-s.closingChannel:
				break
			default:
				var msg []byte
				_, err := s.Conn.Read(msg)
				if err != nil {
					break
				}
				s.messages = append(s.messages, msg)
			}
		}
	}()

	return nil
}

func (s *StompMockClient) Disconnect() error {
	err := s.Conn.Close()
	s.closingChannel <- true
	s.calls = append(s.calls, "Disconnect")
	return err
}

func (s *StompMockClient) SubscribeToQueue(queueName string, messageChanel *chan []byte) error {
	s.calls = append(s.calls, "Subscribe to: "+queueName)
	s.messageChanel[queueName] = messageChanel
	return nil
}

func (s *StompMockClient) Unsubscribe(queueName string) error {
	s.calls = append(s.calls, "Unsubscribe from "+queueName)
	close(*s.messageChanel[queueName])
	delete(s.messageChanel, queueName)
	return nil
}

func (s *StompMockClient) SendMessageToQueue(queueName, contentType string, body []byte) error {
	s.calls = append(s.calls, "Sent message")
	s.messages = append(s.messages, body)
	go func(msg []byte) {

		if s.messageChanel[queueName] == nil {
			channel := make(chan []byte)
			s.messageChanel[queueName] = &channel
		}

		*s.messageChanel[queueName] <- msg
	}(body)
	return nil
}

func (s *StompMockClient) GetMessages() [][]byte {
	return s.messages
}

func (s *StompMockClient) GetCalls() []string {
	return s.calls
}
