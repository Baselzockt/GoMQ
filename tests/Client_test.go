package tests

import (
	"github.com/Baselzockt/GoMQ/client/impl"
	"github.com/Baselzockt/GoMQ/content"
	"github.com/go-stomp/stomp/testutil"
	"gopkg.in/check.v1"
	"testing"
)

func TestSendMessage(t *testing.T) {
	t.Run("MOCK", func(t *testing.T) {
		var client = impl.NewMockClient()
		client.Connect("test")
		want := "test"
		client.SendMessageToQueue("test", content.TEXT, []byte(want))

		got := string(client.GetMessages()[0])
		client.Disconnect()

		if client.GetCalls()[0] != "Connect to test" {
			t.Errorf("Did not connect")
		}

		if client.GetCalls()[1] != "Sent message" {
			t.Errorf("Could not send message")
		}

		if client.GetCalls()[2] != "Disconnect" {
			t.Errorf("Could not send message")
		}

		if got != want {
			t.Errorf("Got %s want %s", got, want)
		}
	})
	t.Run("STOMP", func(t *testing.T) {
		cl, svr := testutil.NewFakeConn(&check.C{})
		var client = impl.NewStompMockClient(cl)
		assertError(t, client.Connect(svr.LocalAddr().String()))

		want := "test"
		assertError(t, client.SendMessageToQueue("test", content.TEXT, []byte(want)))

		if client.GetCalls()[0] != "Connect to: "+svr.LocalAddr().String() {
			t.Errorf("Did not connect")
		}

		if client.GetCalls()[1] != "Sent message" {
			t.Errorf("Could not send message")
		}

		got := string(client.GetMessages()[0])

		assertError(t, client.Disconnect())

		if client.GetCalls()[2] != "Disconnect" {
			t.Errorf("Could not send message")
		}

		if got != want {
			t.Errorf("Got %s want %s", got, want)
		}
	})

}

func TestReceivingMessage(t *testing.T) {
	t.Run("MOCK", func(t *testing.T) {
		var client = impl.NewMockClient()
		want := "test"
		client.SendMessageToQueue("test", content.TEXT, []byte(want))
		channel := make(chan []byte)
		client.SubscribeToQueue("test", channel)

		got := string(<-channel)

		if client.GetCalls()[0] != "Sent message" {
			t.Errorf("Could not send message")
		}

		if client.GetCalls()[1] != "Subscribe to test" {
			t.Errorf("Could not subscribe to queue")
		}

		if got != want {
			t.Errorf("Got %s want %s", got, want)
		}
	})
	t.Run("STOMP", func(t *testing.T) {
		cl, svr := testutil.NewFakeConn(&check.C{})
		var client = impl.NewStompMockClient(cl)
		assertError(t, client.Connect(svr.LocalAddr().String()))

		want := "test"
		assertError(t, client.SendMessageToQueue("test", content.TEXT, []byte(want)))
		channel := make(chan []byte)
		client.SubscribeToQueue("test", &channel)

		got := string(<-channel)

		assertError(t, client.Disconnect())

		if client.GetCalls()[2] != "Subscribe to: test" {
			t.Errorf("Could not subscribe to queue")
		}

		if got != want {
			t.Errorf("Got %s want %s", got, want)
		}
	})
}

func assertError(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Error(err.Error())
	}
}
