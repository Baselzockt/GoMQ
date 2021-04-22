package tests

import (
	"github.com/Baselzockt/GoMQ/client/impl"
	"github.com/Baselzockt/GoMQ/content"
	"testing"
)

func TestSendAndReceive(t *testing.T) {
	client := impl.StompClient{Url: "localhost:61613"}
	err := client.Connect()
	checkForError(t, err)
	channel := make(chan []byte)
	err = client.SubscribeToQueue("test", &channel)
	checkForError(t, err)
	want := "Integration test :D"
	client.SendMessageToQueue("test", content.TEXT, []byte(want))
	got := string(<-channel)
	if got != want {
		t.Errorf("Got %s want %s", got, want)
	}
	client.Disconnect()
}

func checkForError(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("Could not subscribe to queue because of error: %s", err.Error())
	}
}
