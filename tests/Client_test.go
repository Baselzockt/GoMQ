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
		client.Connect()
		want := "test"
		client.SendMessageToQueue("test", content.TEXT, []byte(want))

		got := string(client.GetMessages()[0])
		client.Disconnect()

		if client.GetCalls()[0] != "Connect" {
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
		var client = impl.StompMockClient{Url: svr.LocalAddr().String(), Conn: cl}
		assertNoError(t, client.Connect())

		want := "test"
		assertNoError(t, client.SendMessageToQueue("test", content.TEXT, []byte(want)))

		if client.GetCalls()[0] != "Connect to: "+svr.LocalAddr().String() {
			t.Errorf("Did not connect")
		}

		if client.GetCalls()[1] != "Sent message" {
			t.Errorf("Could not send message")
		}

		got := string(client.GetMessages()[0])

		assertNoError(t, client.Disconnect())

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
		err := client.Connect()
		assertNoError(t, err)
		err = client.SendMessageToQueue("test", content.TEXT, []byte(want))
		assertNoError(t, err)
		channel := make(chan []byte)
		err = client.SubscribeToQueue("test", &channel)
		assertNoError(t, err)

		got := string(<-channel)

		client.Unsubscribe("test")

		if client.GetCalls()[1] != "Sent message" {
			t.Errorf("Could not send message")
		}

		if client.GetCalls()[2] != "Subscribe to test" {
			t.Errorf("Could not subscribe to queue")
		}

		if client.GetCalls()[3] != "Unsubscribe from test" {
			t.Errorf("Could not unsubscribe")
		}

		if got != want {
			t.Errorf("Got %s want %s", got, want)
		}
	})
	t.Run("STOMP", func(t *testing.T) {
		cl, svr := testutil.NewFakeConn(&check.C{})
		var client = impl.StompMockClient{Url: svr.LocalAddr().String(), Conn: cl}
		assertNoError(t, client.Connect())

		want := "test"
		assertNoError(t, client.SendMessageToQueue("test", content.TEXT, []byte(want)))
		channel := make(chan []byte)
		err := client.SubscribeToQueue("test", &channel)
		assertNoError(t, err)

		got := string(<-channel)
		err = client.Unsubscribe("test")
		assertNoError(t, err)
		assertNoError(t, client.Disconnect())

		client2 := impl.StompMockClient{Url: "wroooong", Conn: cl}
		assertError(t, client2.Connect())

		if client.GetCalls()[2] != "Subscribe to: test" {
			t.Errorf("Could not subscribe to queue")
		}

		if client.GetCalls()[3] != "Unsubscribe from test" {
			t.Errorf("Could not unsubscribe got: %s", client.GetCalls()[3])
		}

		if got != want {
			t.Errorf("Got %s want %s", got, want)
		}
	})
	t.Run("STOMP negative test", func(t *testing.T) {
		var client = impl.StompClient{Url: "wrooooooooooong"}
		assertError(t, client.Connect())
		assertError(t, client.SendMessageToQueue("test", content.TEXT, []byte("Wrooong")))
		assertError(t, client.SubscribeToQueue("test", nil))
		assertError(t, client.Disconnect())
		assertError(t, client.Unsubscribe("test"))
	})
}

func assertNoError(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Error(err.Error())
	}
}

func assertError(t testing.TB, err error) {
	t.Helper()
	if err == nil {
		t.Error(err.Error())
	}
}
