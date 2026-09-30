package sdk

import (
	"context"
	"time"
)

type TxnStatus int

// TransactionalSession is returned by Session.BeginTransaction — Commit and
// Abort operate on it. RunInTransaction is the preferred helper for the
// common retry-on-conflict case (D-11); Begin/Commit/Abort stay available
// for manual control.
type TransactionalSession struct{}

func (tx *TransactionalSession) Commit(ctx context.Context) (TxnStatus, error) {
	return 0, nil
}

// Abort is also known as Rollback in some client docs; this package only
// exposes one name.
func (tx *TransactionalSession) Abort(ctx context.Context) error {
	return nil
}

type TxnOption struct{}

func WithMaxAttempts(n int) TxnOption {
	return TxnOption{}
}

func WithRetryDelay(d time.Duration) TxnOption {
	return TxnOption{}
}
