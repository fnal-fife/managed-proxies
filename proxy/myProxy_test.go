package proxy

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeMyProxyer struct {
	err error
}

func (f *fakeMyProxyer) storeInMyProxy(context.Context, string, string, time.Duration) error {
	return f.err
}

// TestStoreInMyProxy verifies that the errors we receive from StoreInMyProxy are valid
func TestStoreInMyProxy(t *testing.T) {
	tests := []*fakeMyProxyer{
		&fakeMyProxyer{nil},
		&fakeMyProxyer{errors.New("Failure")},
	}
	ctx := context.Background()
	valid, _ := time.ParseDuration("24h")

	for _, test := range tests {
		err := StoreInMyProxy(ctx, test, "", "", valid)
		if errorString(err) != errorString(test.err) {
			t.Errorf("Expected and actual errors do not match.  Expected %s, got %s", test.err, err)
		}
	}

}

// Allow us to check values of nil errors against each other
func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
