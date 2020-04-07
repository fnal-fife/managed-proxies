package utils

import (
	"errors"
	"testing"
)

// TestCheckRetrievers makes sure that given differing sets of retrievers and defaultRetrievers, we get the correct error
func TestCheckRetrievers(t *testing.T) {
	tests := []struct {
		retrievers, defaultRetrievers string
		expectedError                 error
	}{
		{
			retrievers:        "foo",
			defaultRetrievers: "foo",
			expectedError:     nil,
		},
		{
			retrievers:        "foo",
			defaultRetrievers: "bar",
			expectedError:     errors.New("Attention:  The retrievers on the jobsub server do not match the default retrievers"),
		},
	}

	for _, test := range tests {
		if err := CheckRetrievers(test.retrievers, test.defaultRetrievers); errorString(err) != errorString(test.expectedError) {
			t.Errorf("Expected errors didn't match:  wanted %s, got %s", test.expectedError, err)
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
