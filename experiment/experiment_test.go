package experiment

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestGetVomsProxyersForExperiment(t *testing.T) {
	type testConfig struct {
		configCertPath string
		accounts       map[string]string
	}

	ctx := context.Background()

	tests := []struct {
		t             testConfig
		expectedError error
	}{
		{
			t: testConfig{
				configCertPath: "/tmp/nowaythiswouldeverexist",
				accounts: map[string]string{
					"account1": "role1",
					"account2": "role2",
				},
			},
			expectedError: errors.New("Could not get DN from certificate"),
		},
	}

	for _, test := range tests {
		if _, err := getVomsProxyersForExperiment(ctx, test.t.configCertPath, test.t.configCertPath, test.t.configCertPath, test.t.accounts); err.Error() != test.expectedError.Error() {
			t.Errorf("Expected error did not match:  Wanted %s, got %s", test.expectedError, err)
		}
	}
}

func TestSetCertKeyLocation(t *testing.T) {
	// We're only testing the cert part, since the key code does exactly the same thing
	tests := []struct {
		certBaseDir, certPath, account, expectedCertFile string
	}{
		{ // If I give a certPath, use that no matter what
			certBaseDir:      "/tmp",
			certPath:         "mytestcert",
			account:          "myaccount",
			expectedCertFile: "mytestcert",
		},
		{
			certBaseDir:      "/tmp",
			certPath:         "",
			account:          "myaccount",
			expectedCertFile: "/tmp/myaccount.cert",
		},
	}

	for _, test := range tests {
		if testCert, _ := setCertKeyLocation(test.certBaseDir, test.certPath, test.certPath, test.account); testCert != test.expectedCertFile {
			t.Errorf("Did not get expected cert location:  Wanted %s, got %s", test.expectedCertFile, testCert)
		}
	}
}

func TestGetKerbTicket(t *testing.T) {
	ctxTout, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	//	var kinitExecutable string
	//	kinitExecutable, err := exec.LookPath("kinit")
	//	if err != nil {
	//		t.Errorf("Could not find kinit executable in path")
	//	}
	//	t.Log(kinitExecutable)

	tests := []struct {
		ctx           context.Context
		krbConfig     KerbConfig
		expectedError error
	}{
		{
			ctx: context.Background(),
			krbConfig: KerbConfig(map[string]string{
				"kinitargs":  "--help",
				"krb5ccname": "/tmp/blah",
			}),
			expectedError: nil,
		},
		{
			ctx: context.Background(),
			krbConfig: KerbConfig(map[string]string{
				"kinitargs":  "-nowaythiswillwork",
				"krb5ccname": "/tmp/blah",
			}),
			expectedError: errors.New("exit status 1"),
		},
		{
			ctx: ctxTout,
			krbConfig: KerbConfig(map[string]string{
				"kinitargs":  "--help",
				"krb5ccname": "/tmp/blah",
			}),
			expectedError: errors.New("context deadline exceeded"),
		},
	}

	for _, test := range tests {
		if err := getKerbTicket(test.ctx, test.krbConfig); errorString(err) != errorString(test.expectedError) {
			t.Errorf("Expected errors did not match:  wanted %s, got %s", test.expectedError, err)
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
