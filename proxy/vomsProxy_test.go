package proxy

import (
	"context"
	"errors"
	"io/ioutil"
	"strconv"
	"testing"
	"time"
)

type fakeVomsProxy struct {
	err error
}

func (f *fakeVomsProxy) getVomsProxy(ctx context.Context, vomsFQAN string) (*VomsProxy, error) {
	return &VomsProxy{}, f.err
}

// TestNewVomsProxy ensures that NewVomsProxy returns the appropriate errors given valid/invalid input
func TestNewVomsProxy(t *testing.T) {
	tests := []struct {
		v   VomsProxyer
		err error
	}{
		{
			v:   &fakeVomsProxy{errors.New("This failed for some reason")},
			err: errors.New("Could not generate a VOMS proxy"),
		},
		{
			v:   &fakeVomsProxy{err: nil},
			err: nil,
		},
	}

	ctx := context.Background()
	for _, test := range tests {
		if _, err := NewVomsProxy(ctx, test.v, ""); errorString(err) != errorString(test.err) {
			t.Errorf("NewVomsProxy test should have returned %s; got %s instead", test.err, err)
		}
	}

}

// TestRemoveVomsProxy checks that VOMS proxy files are properly deleted
func TestRemoveVomsProxy(t *testing.T) {
	tmpLocation, _ := ioutil.TempFile("", "proxytest")
	tests := []struct {
		v   *VomsProxy
		err error
	}{
		{
			v: &VomsProxy{
				Path: tmpLocation.Name(),
			},
			err: nil,
		},
		{
			v: &VomsProxy{
				Path: strconv.FormatInt(time.Now().UnixNano(), 36),
			},
			err: errors.New("VOMS Proxy file does not exist"),
		},
	}

	for _, test := range tests {
		err := test.v.Remove()
		if errorString(err) != errorString(test.err) {
			t.Errorf("Expected and actual errors do not match.  Expected %s, got %s", test.err, err)
		}
	}
}
