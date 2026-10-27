package stream

import (
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestTransactionRecordExplainsFailedOperation(t *testing.T) {
	payment := xdr.PaymentResult{Code: xdr.PaymentResultCodePaymentUnderfunded}
	inner := xdr.OperationResultTr{Type: xdr.OperationTypePayment, PaymentResult: &payment}
	op, err := xdr.NewOperationResult(xdr.OperationResultCodeOpInner, inner)
	if err != nil {
		t.Fatal(err)
	}
	resultUnion, err := xdr.NewTransactionResultResult(xdr.TransactionResultCodeTxFailed, []xdr.OperationResult{op})
	if err != nil {
		t.Fatal(err)
	}
	result := xdr.TransactionResult{Result: resultUnion}
	encoded, err := xdr.MarshalBase64(&result)
	if err != nil {
		t.Fatal(err)
	}
	info := protocol.TransactionInfo{TransactionDetails: protocol.TransactionDetails{Status: protocol.TransactionStatusFailed, ResultXDR: encoded}}
	record := transactionRecord(info)
	if record.Transaction.ErrorCode != "payment_underfunded" {
		t.Fatalf("error code = %q", record.Transaction.ErrorCode)
	}
	if record.Transaction.FailureReason != "payment underfunded" {
		t.Fatalf("detail = %q", record.Transaction.FailureReason)
	}
	if record.Transaction.FailedOpIndex == nil || *record.Transaction.FailedOpIndex != 0 {
		t.Fatalf("failed op index = %v", record.Transaction.FailedOpIndex)
	}
	if record.Transaction.FailedOpType != "payment" {
		t.Fatalf("failed op type = %q", record.Transaction.FailedOpType)
	}
}
