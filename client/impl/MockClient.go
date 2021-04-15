package impl

type MockClient struct {
	calls         []string
	messages      [][]byte
	messageChanel map[string]chan []byte
}

func NewMockClient() *MockClient {
	return &MockClient{[]string{}, [][]byte{}, map[string]chan []byte{}}
}

func (m *MockClient) Connect(url string) error {
	m.calls = append(m.calls, "Connect to "+url)
	return nil
}

func (m *MockClient) Disconnect() error {
	m.calls = append(m.calls, "Disconnect")
	return nil
}

func (m *MockClient) SubscribeToQueue(queueName string, messageChanel *chan []byte) error {
	m.calls = append(m.calls, "Subscribe to "+queueName)
	m.messageChanel[queueName] = *messageChanel
	return nil
}

func (m *MockClient) Unsubscribe(queueName string) error {
	m.calls = append(m.calls, "Unsubscribe from "+queueName)
	close(m.messageChanel[queueName])
	delete(m.messageChanel, queueName)
	return nil
}

func (m *MockClient) SendMessageToQueue(queueName, contentType string, body []byte) error {
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
