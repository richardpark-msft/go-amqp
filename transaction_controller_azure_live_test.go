package amqp_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/require"
)

var liveTestCheck = func(t *testing.T) {}

func init() {
	_, err := newTestVars()

	if err != nil {
		fmt.Printf("Errors loading test vars, live tests will be disabled: %s", err)

		liveTestCheck = func(t *testing.T) {
			t.Skipf("Live tests are disabled, environment not defined")
		}
		return
	}

	// t.Skipf("Skipping live tests, .env file not setup")

	// purge the queue before all the tests start.
	testClients := mustCreateClients()

	defer func() {
		if err := testClients.Cleanup(); err != nil {
			panic(err)
		}
	}()

	// purge all the old messages
	msgs, err := receiveMessages(context.Background(), testClients, &receiveEventsOptions{
		MaxWaitForMessage: 10 * time.Second,
	})

	if err != nil {
		panic(err)
	}

	if len(msgs) > 0 {
		fmt.Printf("(init) Purged %d messages\n", len(msgs))
	}
}

func TestTransactionControllerDeclare(t *testing.T) {
	liveTestCheck(t)

	clients := mustCreateClients()

	defer func() {
		err := clients.Cleanup()
		require.NoError(t, err)
	}()

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

func TestTransactionControllerDeclareAndDischargeNoMessages(t *testing.T) {
	liveTestCheck(t)

	clients := mustCreateClients()

	defer func() {
		err := clients.Cleanup()
		require.NoError(t, err)
	}()

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

func TestTransactionControllerDeclareAndDischarge(t *testing.T) {
	liveTestCheck(t)

	clients := mustCreateClients()

	defer func() {
		err := clients.Cleanup()
		require.NoError(t, err)
	}()

	now := time.Now().UnixNano()

	// use the same transaction controller for three transactions, serially.
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		transactionID, err := clients.TC.Declare(ctx, amqp.TransactionDeclare{}, nil)
		require.NoError(t, err)
		require.NotEmpty(t, transactionID)

		body := fmt.Sprintf(t.Name()+": %d, i:%d", now, i)

		err = sendTestMessage(t, clients, &amqp.Message{Value: body, TransactionID: transactionID})
		require.NoError(t, err)

		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = clients.TC.Discharge(ctx, amqp.TransactionDischarge{
			TransactionID: transactionID,
		}, nil)
		require.NoError(t, err)

		t.Logf("Waiting to receive message we commited in the transaction")
		messages, err := receiveMessages(context.Background(), clients, nil)
		require.NoError(t, err)
		require.Equal(t, body, messages[0].Value)
		require.Equal(t, 1, len(messages))
	}
}

func TestTransactionControllerDischargingWithInvalidID(t *testing.T) {
	liveTestCheck(t)

	clients := mustCreateClients()

	defer func() {
		err := clients.Cleanup()
		require.NoError(t, err)
	}()

	// this is from an older transaction I ran. Won't be valid anymore.
	id := []byte("txn:1e99912c4f2849d6845ecb72e321795b__G65:3417454_G65")

	// Discharging twice:
	// *Error{Condition: amqp:transaction:unknown-id, Description: Transaction is not declared Reference:638f20d1-d269-4579-b819-45782f70347e,
	// TrackingId:68f7ab2d-700a-45d7-9873-51ed47cc55c7_G34, SystemTracker:gtm, Timestamp:2023-08-28T22:57:19, Info: map[]}
	err := clients.TC.Discharge(context.Background(), amqp.TransactionDischarge{
		TransactionID: id,
	}, nil)
	requireAMQPError(t, err, amqp.ErrCondTransactionUnknownID)
}

func TestTransactionControllerDeclareWithGlobalID(t *testing.T) {
	liveTestCheck(t)

	clients := mustCreateClients()

	defer func() {
		err := clients.Cleanup()
		require.NoError(t, err)
	}()

	transactionID, err := clients.TC.Declare(context.Background(), amqp.TransactionDeclare{
		// this was just ignored by Service Bus...
		GlobalID: []byte("some global ID"),
	}, nil)
	require.NoError(t, err)
	require.NotNil(t, transactionID)
}

// TestTransactionControllerImplicitRollback checks that if you just close your link involved in the transaction
// that the default is to rollback.
func TestTransactionControllerImplicitRollback(t *testing.T) {
	liveTestCheck(t)

	clients := mustCreateClients()

	defer func() {
		err := clients.Cleanup()
		require.NoError(t, err)
	}()

	transactionID, err := clients.TC.Declare(context.Background(), amqp.TransactionDeclare{}, nil)
	require.NoError(t, err)

	// send the message, associated with the transaction
	err = sendTestMessage(t, clients, &amqp.Message{Value: "hello", TransactionID: transactionID})
	require.NoError(t, err)

	// shut everything down instead
	err = clients.Cleanup()
	require.NoError(t, err)

	// check that it's clean.
	clients = mustCreateClients()

	messages, err := receiveMessages(context.Background(), clients, nil)
	require.NoError(t, err)
	require.Empty(t, messages)
}

func TestTransactionControllersFromSeparateConnectionsAreIndependent(t *testing.T) {
	liveTestCheck(t)

	clients1 := mustCreateClients()

	t.Cleanup(func() {
		err := clients1.Cleanup()
		require.NoError(t, err)
	})

	otherClients := mustCreateClients()

	t.Cleanup(func() {
		err := otherClients.Cleanup()
		require.NoError(t, err)
	})

	transID, err := clients1.TC.Declare(context.Background(), amqp.TransactionDeclare{}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, transID)

	// attempt to Discharge a transaction on a separate connection than it was created on.
	err = otherClients.TC.Discharge(context.Background(), amqp.TransactionDischarge{
		TransactionID: transID,
	}, nil)
	requireAMQPError(t, err, amqp.ErrCondTransactionUnknownID, "transaction IDs aren't global across connections")

	// now let's try sending a message as part of a transaction from another connection
	err = sendTestMessage(t, otherClients, &amqp.Message{Value: "hello", TransactionID: transID})
	requireAMQPError(t, err, amqp.ErrCondTransactionUnknownID, "transaction IDs aren't global across connections")
}

func TestTransactionControllerMultipleActiveTransactions(t *testing.T) {
	liveTestCheck(t)

	clients := mustCreateClients()

	t.Cleanup(func() {
		err := clients.Cleanup()
		require.NoError(t, err)
	})

	now := time.Now().UnixNano()
	firstMsgValue := fmt.Sprintf("first transaction: %d", now)
	secondMsgValue := fmt.Sprintf("second transaction: %d", now)

	// multiple transactions can be active simultaneously.

	// this first transaction will be completed before the second transaction (which we're about to also
	// create).
	firstTransID, err := clients.TC.Declare(context.Background(), amqp.TransactionDeclare{}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, firstTransID)

	secondTransID, err := clients.TC.Declare(context.Background(), amqp.TransactionDeclare{}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, secondTransID)

	// send a message to each transaction - so now we have pending work in both, simultaneously.
	err = sendTestMessage(t, clients, &amqp.Message{Value: firstMsgValue, TransactionID: firstTransID})
	require.NoError(t, err)

	err = sendTestMessage(t, clients, &amqp.Message{Value: secondMsgValue, TransactionID: secondTransID})
	require.NoError(t, err)

	{
		// now we'll commit the first transaction. The second transaction remains uncommitted.
		err = clients.TC.Discharge(context.Background(), amqp.TransactionDischarge{
			TransactionID: firstTransID,
		}, nil)
		require.NoError(t, err)

		// and now the message is available.
		messages, err := receiveMessages(context.Background(), clients, &receiveEventsOptions{
			MaxWaitForMessage: time.Minute,
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(messages))

		// we get the message from our first transaction
		require.Equal(t, firstMsgValue, messages[0].Value)
	}

	{
		// now we'll commit the second transaction, and validate that it's event is still available.
		err = clients.TC.Discharge(context.Background(), amqp.TransactionDischarge{
			TransactionID: secondTransID,
		}, nil)
		require.NoError(t, err)

		messages, err := receiveMessages(context.Background(), clients, nil)
		require.NoError(t, err)
		require.Equal(t, 1, len(messages))

		require.Equal(t, secondMsgValue, messages[0].Value)
	}
}

func TestTransactionControllerTimeout(t *testing.T) {
	liveTestCheck(t)

	clients := mustCreateClients()

	defer func() {
		err := clients.Cleanup()
		require.NoError(t, err)
	}()

	defer func() {
		// purge all the excess events.
		_, err := receiveMessages(context.Background(), clients, nil)
		require.NoError(t, err)
	}()

	tc, err := clients.Session.NewTransactionController(context.Background(), nil)
	require.NoError(t, err)

	// transactions (with Service Bus) can only last for two minutes.
	// Hm...
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	transactionID, err := tc.Declare(ctx, amqp.TransactionDeclare{}, nil)
	require.NoError(t, err)
	require.NotEmpty(t, transactionID)

	err = sendTestMessage(t, clients, &amqp.Message{Value: "hello", TransactionID: transactionID})
	require.NoError(t, err)

	// > The transaction timer starts when the first operation in the transaction starts.
	start := time.Now()

	// We can't be totally idle or else they'll shut down the connection.
	// We want it to be alive, with the transaction just sitting there un-discharged.
	{
		ctx, cancel := context.WithDeadline(context.Background(), start.Add(2*time.Minute+30*time.Second))
		defer cancel()

		go func() {
			for {
				select {
				case <-ctx.Done():
				default:
					err = sendTestMessage(t, clients, &amqp.Message{Value: "hello"})
					require.NoError(t, err)

					time.Sleep(time.Minute)
				}
			}
		}()

		<-ctx.Done()
	}

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

type testVars struct {
	Endpoint  string
	User      string
	Password  string
	QueueName string
}

func newTestVars() (testVars, error) {
	if err := godotenv.Load(); err != nil {
		return testVars{}, err
	}

	var missing []string

	getEnv := func(ev string) string {
		v := os.Getenv(ev)
		if v == "" {
			missing = append(missing, ev)
		}
		return v
	}

	tv := testVars{
		Endpoint:  getEnv("AMQP_ENDPOINT"), // ex: amqps://<your-service-bus-instance>.servicebus.windows.net
		User:      getEnv("AMQP_USER"),     // ex: RootManageSharedAccessKey
		Password:  getEnv("AMQP_PASSWORD"), // the key value
		QueueName: getEnv("AMQP_QUEUE"),    // ex: demo
	}

	if tv.Endpoint == "" || tv.User == "" || tv.Password == "" || tv.QueueName == "" {
		return testVars{}, fmt.Errorf("Missing environment variables: %s", strings.Join(missing, ","))
	}

	return tv, nil
}

func mustCreateClients() clients {
	getEnv := func(ev string) string {
		v := os.Getenv(ev)
		if v == "" {
			log.Fatalf("%s is NOT defined in environment", ev)
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

type receiveEventsOptions struct {
	MaxWaitForMessage time.Duration
}

func receiveMessages(ctx context.Context, clients clients, opts *receiveEventsOptions) ([]*amqp.Message, error) {
	if opts == nil {
		opts = &receiveEventsOptions{}
	}

	if opts.MaxWaitForMessage == 0 {
		opts.MaxWaitForMessage = 5 * time.Second
	}

	session, err := clients.Conn.NewSession(context.Background(), nil)

	if err != nil {
		return nil, err
	}

	defer func() {
		err := session.Close(context.Background())

		if err != nil {
			panic(err)
		}
	}()

	receiver, err := session.NewReceiver(context.Background(), clients.QueueName, &amqp.ReceiverOptions{
		Credit:         1000,
		SettlementMode: amqp.ReceiverSettleModeSecond.Ptr(),
	})

	if err != nil {
		return nil, err
	}

	var msgs []*amqp.Message

	for {
		ctx, cancel := context.WithTimeout(ctx, opts.MaxWaitForMessage)
		msg, err := receiver.Receive(ctx, nil)
		cancel()

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			// probably empty
			break
		}

		if err := receiver.AcceptMessage(context.Background(), msg); err != nil {
			return nil, err
		}

		msgs = append(msgs, msg)
	}

	return msgs, nil
}

func requireAMQPError(t *testing.T, err error, expectedCond amqp.ErrCond, msgAndArgs ...interface{}) {
	var amqpErr *amqp.Error
	require.ErrorAs(t, err, &amqpErr, msgAndArgs...)
	require.Equal(t, expectedCond, amqpErr.Condition, msgAndArgs...)
}

func sendTestMessage(t *testing.T, clients clients, msg *amqp.Message) error {
	session, err := clients.Conn.NewSession(context.Background(), nil)
	require.NoError(t, err)
	defer testClose(t, session.Close)

	sender, err := session.NewSender(context.Background(), clients.QueueName, nil)
	require.NoError(t, err)

	return sender.Send(context.Background(), msg, nil)
}
