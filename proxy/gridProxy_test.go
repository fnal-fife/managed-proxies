package proxy

import (
	"context"
	"errors"
	"io/ioutil"
	"strconv"
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
		g   GridProxyer
		err error
	}{
		{
			g:   &fakeGridProxy{errors.New("This failed for some reason")},
			err: errors.New("Could not get a new grid proxy from gridProxyer"),
		},
		{
			g:   &fakeGridProxy{err: nil},
			err: nil,
		},
	}

	ctx := context.Background()
	valid, _ := time.ParseDuration(defaultValidity)
	for _, test := range tests {
		if _, err := NewGridProxy(ctx, test.g, valid); errorString(err) != errorString(test.err) {
			t.Errorf("NewGridProxy test should have returned %s; got %s instead", test.err, err)
		}
	}

}

// TestRemoveGridProxy make sure that GridProxy.Remove() behaves as we expect in case a file doesn't exist, etc.
func TestRemoveGridProxy(t *testing.T) {
	tmpLocation, _ := ioutil.TempFile("", "proxytest")
	tests := []struct {
		g   *GridProxy
		err error
	}{
		{
			g: &GridProxy{
				Path: tmpLocation.Name(),
			},
			err: nil,
		},
		{
			g: &GridProxy{
				Path: strconv.FormatInt(time.Now().UnixNano(), 36),
			},
			err: errors.New("Grid proxy file does not exist"),
		},
	}

	for _, test := range tests {
		err := test.g.Remove()
		if errorString(err) != errorString(test.err) {
			t.Errorf("Expected and actual errors do not match.  Expected %s, got %s", test.err, err)
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
