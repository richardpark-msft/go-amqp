package amqp

import (
	"context"
	"fmt"

	"github.com/Azure/go-amqp/internal/encoding"
)

type TransactionCoordinatorOptions struct {
	// Capabilities is the list of extension capabilities the sender supports.
	Capabilities []string
}

func newTransactionController(session *Session, opts *TransactionCoordinatorOptions) (*TransactionController, error) {
	if opts == nil {
		opts = &TransactionCoordinatorOptions{}
	}

	sender, err := newSender("", session, &SenderOptions{
		Capabilities: opts.Capabilities,
	})

	if err != nil {
		return nil, err
	}

	return &TransactionController{sender: sender}, nil
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
	deliveryState, err := tc.sender.sendEx(ctx, &Message{
		Value: declare,
	}, opts)

	if err != nil {
		return nil, err
	}

	if state, ok := deliveryState.(*encoding.StateDeclared); ok {
		return state.TransactionID, nil
	}

	return nil, fmt.Errorf("unexpected delivery state %T when DECLAREing transaction", deliveryState)
}
