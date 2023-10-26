package amqp

import (
	"context"
)

type TransactionControllerOptions struct {
	// Capabilities is the list of extension capabilities the sender supports.
	Capabilities []string
}

// TransactionController can interact with the transaction coordinator
// for a transactional resource.
// Reference: http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transactions-v1.0-os.html#section-coordination
type TransactionController struct {
	sender *Sender
}

func (tc *TransactionController) Discharge(ctx context.Context, discharge TransactionDischarge, opts *SendOptions) error {
	return tc.sender.Send(ctx, &Message{
		Value: discharge,
	}, opts)
}

func (tc *TransactionController) Declare(ctx context.Context, declare TransactionDeclare, opts *SendOptions) (any, error) {
	return tc.sender.declareTransaction(ctx, &Message{
		Value: declare,
	}, opts)
}

func (tc *TransactionController) Close(ctx context.Context) error {
	return tc.sender.Close(ctx)
}
