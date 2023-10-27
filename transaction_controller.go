package amqp

import (
	"context"
	"fmt"

	"github.com/Azure/go-amqp/internal/encoding"
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
	state, err := tc.sender.sendRaw(ctx, &Message{
		Value: declare,
	}, opts)

	if err != nil {
		return nil, err
	}

	declared, ok := state.(*encoding.StateDeclared)

	if !ok {
		return nil, fmt.Errorf("invalid response when declaring transaction (not *StateDeclared, was %T)", state)
	}

	return declared.TransactionID, nil
}

func (tc *TransactionController) Close(ctx context.Context) error {
	return tc.sender.Close(ctx)
}
