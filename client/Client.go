package client

type Client interface {
	Connect() error
	Disconnect() error
	SubscribeToQueue(queueName string, messageChanel *chan []byte) error
	Unsubscribe(queueName string) error
	SendMessageToQueue(queueName, contentType string, body []byte) error
}
