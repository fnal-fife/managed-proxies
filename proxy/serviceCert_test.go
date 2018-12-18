package proxy

import (
	"context"
	"errors"
	"testing"
)

type fakeServiceCert struct {
	subjectFunc func() (string, error)
}

func (f *fakeServiceCert) getCertPath() string                                { return "Path to cert" }
func (f *fakeServiceCert) getKeyPath() string                                 { return "Path to key" }
func (f *fakeServiceCert) getCertSubject(ctx context.Context) (string, error) { return f.subjectFunc() }

func TestGetDN(t *testing.T) {

	tests := []struct {
		f             *fakeServiceCert
		expectedDN    string
		expectedError error
	}{
		{
			f: &fakeServiceCert{
				subjectFunc: func() (string, error) {
					return "This worked", nil
				},
			},
			expectedDN:    "This worked",
			expectedError: nil,
		},
		{
			f: &fakeServiceCert{
				subjectFunc: func() (string, error) {
					return "", errors.New("This did not work")
				},
			},
			expectedDN:    "",
			expectedError: errors.New("This did not work"),
		},
		{
			f: &fakeServiceCert{
				subjectFunc: func() (string, error) {
					return "This should still not work", errors.New("This really should not work")
				},
			},
			expectedDN:    "",
			expectedError: errors.New("This really should not work"),
		},
	}

	ctx := context.Background()
	for _, test := range tests {
		dn, err := GetDN(ctx, test.f)
		if dn != test.expectedDN {
			t.Errorf("Expected DNs did not match: wanted %s, got %s", test.expectedDN, dn)
		}
		if errorString(err) != errorString(test.expectedError) {
			t.Errorf("Expected errors did not match: wanted %s, got %s", test.expectedError, err)
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
