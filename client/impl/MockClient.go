package impl

import "errors"

type MockClient struct {
	calls          []string
	messages       [][]byte
	messageChanel  map[string]chan []byte
	connected      bool
	errorOnConnect bool
}

func NewMockClient() *MockClient {
	return &MockClient{[]string{}, [][]byte{}, map[string]chan []byte{}, false, false}
}

func (m *MockClient) Connect() error {
	if m.errorOnConnect {
		return errors.New("could not connect")
	}

	m.calls = append(m.calls, "Connect")
	m.connected = true
	return nil
}

func (m *MockClient) Disconnect() error {
	m.calls = append(m.calls, "Disconnect")
	m.connected = false
	return nil
}

func (m *MockClient) SubscribeToQueue(queueName string, messageChanel *chan []byte) error {

	if !m.connected {
		return errors.New("client not connected")
	}

	m.calls = append(m.calls, "Subscribe to "+queueName)
	m.messageChanel[queueName] = *messageChanel
	return nil
}

func (m *MockClient) Unsubscribe(queueName string) error {
	if !m.connected {
		return errors.New("client not connected")
	}

	m.calls = append(m.calls, "Unsubscribe from "+queueName)
	close(m.messageChanel[queueName])
	delete(m.messageChanel, queueName)
	return nil
}

func (m *MockClient) SetErrorOnReconnect(errorOnConnect bool) {
	m.errorOnConnect = errorOnConnect
}

func (m *MockClient) SendMessageToQueue(queueName, contentType string, body []byte) error {
	if !m.connected {
		return nil
	}

	m.calls = append(m.calls, "Sent message")
	m.messages = append(m.messages, body)
	go func(msg []byte) {
		m.messageChanel[queueName] <- msg
	}(body)
	return nil
}

func (m *MockClient) GetMessages() [][]byte {
	return m.messages
}

func (m *MockClient) GetCalls() []string {
	return m.calls
}
