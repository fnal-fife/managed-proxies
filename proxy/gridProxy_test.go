package proxy

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

// Satisfy GridProxyer interface
type fakeGridProxy struct {
	err error
}

func (f *fakeGridProxy) getGridProxy(ctx context.Context, valid time.Duration) (*GridProxy, error) {
	return &GridProxy{}, f.err
}

// TestNewGridProxy tests that we get the expected errors back from NewGridProxy
func TestNewGridProxy(t *testing.T) {
	tests := []struct {
		g   getGridProxyer
		err error
	}{
		{
			g:   &fakeGridProxy{errors.New("This failed for some reason")},
			err: &NewGridProxyError{"Could not get a new grid proxy from gridProxyer"},
		},
		{
			g:   &fakeGridProxy{err: nil},
			err: nil,
		},
	}

	ctx := context.Background()
	valid, _ := time.ParseDuration(defaultValidity)
	for _, test := range tests {
		_, _, err := NewGridProxy(ctx, test.g, valid)
		if reflect.TypeOf(err) != reflect.TypeOf(test.err) {
			t.Errorf("NewGridProxy test should have returned %T; got %T instead", test.err, err)
		}
	}

}

// TestFmtDurationForGPI verifies that if we give a valid time.Duration, it is formatted correctly
func TestFmtDurationForGPI(t *testing.T) {
	testDurStrings := []string{"24h", "80h2m", "3.5h2.5m"}
	testDurations := make([]time.Duration, 0)

	for _, s := range testDurStrings {
		t, _ := time.ParseDuration(s)
		testDurations = append(testDurations, t)
	}

	tests := []struct {
		inputTime   time.Duration
		expectedOut string
	}{
		{
			inputTime:   testDurations[0],
			expectedOut: "24:00",
		},
		{
			inputTime:   testDurations[1],
			expectedOut: "80:02",
		},
		{
			inputTime:   testDurations[2],
			expectedOut: "3:33",
		},
	}

	for _, test := range tests {
		if result := fmtDurationForGPI(test.inputTime); result != test.expectedOut {
			t.Errorf("Expected outputs did not match.  Wanted %s, got %s", test.expectedOut, result)
		}
	}
}

// Allow us to check values of nil errors against each other TODO: MOve this somewhere else
func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
