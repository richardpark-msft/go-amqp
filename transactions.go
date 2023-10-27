package amqp

import (
	"github.com/Azure/go-amqp/internal/buffer"
	"github.com/Azure/go-amqp/internal/encoding"
)

// TransactionCapability represents capabilities for a transaction coordinator.
type TransactionCapability encoding.Symbol

const (
	// LocalTransactionCapability indicates the transaction coordinator supports local Transactions.
	LocalTransactionCapability TransactionCapability = "amqp:local-transactions"

	// DistributedTransactionCapability indicates the transaction coordinator supports AMQP Distributed Transactions.
	DistributedTransactionCapability TransactionCapability = "amqp:distributed-transactions"

	// PromotableTransactionCapability indicates the transaction coordinator supports AMQP Promotable Transactions.
	PromotableTransactionCapability TransactionCapability = "amqp:promotable-transactions"

	// MultiTxnPerSessionTransactionCapability indicates the transaction coordinator supports multiple active transactions on a single session.
	MultiTxnPerSessionTransactionCapability TransactionCapability = "amqp:multi-txns-per-ssn"
)

type TransactionDeclare struct {
	GlobalID any
}

func (t TransactionDeclare) Marshal(wr *buffer.Buffer) error {
	// TODO: add to typecode
	return encoding.MarshalComposite(wr, encoding.TypeCodeTransactionDeclare, []encoding.MarshalField{
		{Value: t.GlobalID, Omit: t.GlobalID == nil},
	})
}

type TransactionDischarge struct {
	TransactionID any
	// Fail indicates that the work associated with this transaction has failed, and the controller
	// wishes the transaction to be rolled back. If the transaction is associated with a global-id
	// this will render the global transaction rollback-only. If the transaction is a local transaction,
	// then this flag controls whether the transaction is committed or aborted when it is discharged.
	// (Note that the specification for distributed transactions within AMQP 1.0 will be provided
	// separately in Part 6 Distributed Transactions).
	Fail bool
}

func (t TransactionDischarge) Marshal(wr *buffer.Buffer) error {
	return encoding.MarshalComposite(wr, encoding.TypeCodeTransactionDischarge, []encoding.MarshalField{
		{Value: t.TransactionID},
		{Value: t.Fail},
	})
}

// Coordinator is used to setup a Receiver/Sender to act as a transaction coordinator.
//
// <type name="coordinator" class="composite" source="list" provides="target">
//
//	<descriptor name="amqp:coordinator:list" code="0x00000000:0x00000030"/>
//	<field name="capabilities" type="symbol" requires="txn-capability" multiple="true"/>
//
// </type>
// type CoordinatorTarget struct {
// 	// Capabilities for this transaction controller.
// 	// - http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transactions-v1.0-os.html#type-txn-capability
// 	Capabilities []TransactionCapability
// }

// func (ct *CoordinatorTarget) Marshal(wr *buffer.Buffer) error {
// 	return encoding.MarshalComposite(wr, encoding.TypeCodeCoordinatorTarget, []encoding.MarshalField{
// 		{Value: ct.Capabilities, Omit: len(ct.Capabilities) == 0},
// 	})
// }

// NOTES:
// - Transaction controllers SHOULD establish a control link that allows the rejected outcome.
//    http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transactions-v1.0-os.html#type-coordinator:~:text=Transaction%20controllers%20SHOULD%20establish%20a%20control%20link%20that%20allows%20the%20rejected%20outcome.
//
// - The interaction seems pretty simple to start a transaction:
// 		1. ATTACH with a target = Coordinator()
//     	2. Target responds with ATTACH with its capabilities (I believe we have some control over what we request, but for SB it's limited)
//		3. We send TRANSFER frames with an AMQP value of DECLARE.
//		4. Message is DISPOSITIONed on the server-side. So we must wait for the ACK of our Send.
