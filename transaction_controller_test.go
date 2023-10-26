package amqp_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/require"
)

var initTest func(t *testing.T)

func init() {
	if err := godotenv.Load(); err != nil {
		// disable the tests.
		initTest = func(t *testing.T) {
			t.Skipf("Skipping live tests, .env file not setup")
		}
	}

	// purge the queue before all the tests start.
	testClients := mustCreateClients()

	// purge all the old messages
	msgs, err := receiveEvents(testClients.Session, testClients.QueueName)

	if err != nil {
		panic(err)
	}

	if len(msgs) > 0 {
		fmt.Printf("(init) Purged %d messages\n", len(msgs))
	}
}

func TestDeclareController(t *testing.T) {
	clients := mustCreateClients()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	transactionID, err := clients.TC.Declare(ctx, amqp.TransactionDeclare{}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, transactionID)

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = clients.TC.Discharge(ctx, amqp.TransactionDischarge{
		TransactionID: transactionID,
	}, nil)
	require.NoError(t, err)
}

func TestDeclareAndDischarge(t *testing.T) {
	clients := mustCreateClients()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	transactionID, err := clients.TC.Declare(ctx, amqp.TransactionDeclare{}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, transactionID)

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = clients.TC.Discharge(ctx, amqp.TransactionDischarge{
		TransactionID: transactionID,
	}, nil)
	require.NoError(t, err)
}

func TestNormalDeclareAndDischarge(t *testing.T) {
	clients := mustCreateClients()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	transactionID, err := clients.TC.Declare(ctx, amqp.TransactionDeclare{}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, transactionID)

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = clients.TC.Discharge(ctx, amqp.TransactionDischarge{
		TransactionID: transactionID,
	}, nil)
	require.NoError(t, err)
}

func TestDischargingWithInvalidID(t *testing.T) {
	clients := mustCreateClients()

	// this is from an older transaction I ran. Won't be valid anymore.
	id := []byte("txn:1e99912c4f2849d6845ecb72e321795b__G65:3417454_G65")

	// Discharging twice:
	// *Error{Condition: amqp:transaction:unknown-id, Description: Transaction is not declared Reference:638f20d1-d269-4579-b819-45782f70347e,
	// TrackingId:68f7ab2d-700a-45d7-9873-51ed47cc55c7_G34, SystemTracker:gtm, Timestamp:2023-08-28T22:57:19, Info: map[]}
	err := clients.TC.Discharge(context.Background(), amqp.TransactionDischarge{
		TransactionID: id,
	}, nil)
	var amqpErr *amqp.Error
	require.ErrorAs(t, err, &amqpErr)
	require.Equal(t, amqp.ErrCondTransactionUnknownID, amqpErr.Condition)
}

func TestDeclareWithGlobalID(t *testing.T) {
	clients := mustCreateClients()
	transactionID, err := clients.TC.Declare(context.Background(), amqp.TransactionDeclare{
		// this was just ignored by Service Bus...
		GlobalID: "some global ID",
	}, nil)
	require.NoError(t, err)
	require.NotNil(t, transactionID)
}

// TestTransactionImplicitRollback checks that if you just close your link involved in the transaction
// that the default is to rollback.
func TestTransactionImplicitRollback(t *testing.T) {
	clients := mustCreateClients()
	defer func() {
		err := clients.Cleanup() // in case of test failure.
		t.Logf("error in cleanup: %s", err)
	}()

	transactionID, err := clients.TC.Declare(context.Background(), amqp.TransactionDeclare{}, nil)
	require.NoError(t, err)

	// send the message, associated with the transaction
	{
		sender, err := clients.Session.NewSender(context.Background(), clients.QueueName, nil)
		require.NoError(t, err)

		err = sender.Send(context.Background(), &amqp.Message{
			Value:         "value",
			TransactionID: transactionID,
		}, nil)
		require.NoError(t, err)

		err = sender.Close(context.Background())
		require.NoError(t, err)
	}

	// shut everything down instead
	err = clients.Cleanup()
	require.NoError(t, err)

	// check that it's clean.
	clients = mustCreateClients()
}

func TestTransactionPeekThroughTransaction(t *testing.T) {
	t.Fail()
}

func TestTransactionTimeout(t *testing.T) {
	clients := mustCreateClients()

	tc, err := clients.Session.NewTransactionController(context.Background(), nil)
	require.NoError(t, err)

	// transactions (with Service Bus) can only last for two minutes.
	// Hm...
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	transactionID, err := tc.Declare(ctx, amqp.TransactionDeclare{}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, transactionID)

	sender, err := clients.Session.NewSender(context.Background(), clients.QueueName, nil)
	require.NoError(t, err)

	err = sender.Send(context.Background(), &amqp.Message{Value: "hello", TransactionID: transactionID}, nil)
	require.NoError(t, err)

	// > The transaction timer starts when the first operation in the transaction starts.
	start := time.Now()

	// we can't be totally idle...
	backgroundCtx, cancelBackground := context.WithDeadline(context.Background(), start.Add(2*time.Minute+30*time.Second))
	defer cancelBackground()

	go func() {
		for {
			select {
			case <-backgroundCtx.Done():
			default:
				err = sender.Send(backgroundCtx, &amqp.Message{Value: "hello"}, nil)
				require.NoError(t, err)

				time.Sleep(time.Second)
			}
		}
	}()

	<-backgroundCtx.Done()

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = tc.Discharge(ctx, amqp.TransactionDischarge{
		TransactionID: transactionID,
	}, nil)
	require.NoError(t, err)
}

type clients struct {
	Conn      *amqp.Conn
	Session   *amqp.Session
	TC        *amqp.TransactionController
	Cleanup   func() error
	QueueName string
}

func mustCreateClients() clients {
	getEnv := func(ev string) string {
		v := os.Getenv(ev)
		if v == "" {
			log.Fatalf("%s is defined in environment", ev)
		}
		return v
	}

	endpoint := getEnv("AMQP_ENDPOINT") // ex: amqps://<your-service-bus-instance>.servicebus.windows.net
	user := getEnv("AMQP_USER")         // ex: RootManageSharedAccessKey
	password := getEnv("AMQP_PASSWORD") // the key value
	queueName := getEnv("AMQP_QUEUE")   // ex: demo

	// Optional: Useful if you want to view this encrypted traffic using wireshark.
	tlsKeyLogPath := os.Getenv("TLS_KEYLOGPATH")

	var tlsConfig *tls.Config

	if tlsKeyLogPath != "" {
		f, err := os.OpenFile(tlsKeyLogPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0777)

		if err != nil {
			panic(err)
		}

		tlsConfig = &tls.Config{
			KeyLogWriter: f,
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := amqp.Dial(ctx, endpoint, &amqp.ConnOptions{
		SASLType:  amqp.SASLTypePlain(user, password),
		TLSConfig: tlsConfig,
	})
	if err != nil {
		panic(err)
	}

	sess, err := conn.NewSession(ctx, nil)
	if err != nil {
		panic(err)
	}

	tc, err := sess.NewTransactionController(ctx, nil)
	if err != nil {
		panic(err)
	}

	cleanupFn := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := tc.Close(ctx); err != nil {
			return err
		}

		if err := sess.Close(ctx); err != nil {
			return err
		}

		return conn.Close()
	}

	return clients{
		Cleanup:   cleanupFn,
		Conn:      conn,
		QueueName: queueName,
		Session:   sess,
		TC:        tc,
	}
}

func TestNestedTransactionControllers(t *testing.T) {
	panic("Not implemented yet, but I expect this test to fail")
}

func receiveEvents(session *amqp.Session, queueName string) ([]*amqp.Message, error) {
	receiver, err := session.NewReceiver(context.Background(), queueName, &amqp.ReceiverOptions{
		Credit:         100,
		SettlementMode: amqp.ReceiverSettleModeFirst.Ptr(),
	})

	if err != nil {
		panic(err)
	}

	var msgs []*amqp.Message

	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		msg, err := receiver.Receive(ctx, nil)
		cancel()

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			// probably empty
			break
		}

		msgs = append(msgs, msg)
	}

	return msgs, nil
}
