package proxy

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"reflect"
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

func (f *fakeVomsProxy) copyProxy(ctx context.Context, node, acct, dest string) error {
	return f.err
}

// TestNewVomsProxy ensures that NewVomsProxy returns the appropriate errors given valid/invalid input
func TestNewVomsProxy(t *testing.T) {
	tests := []struct {
		v   GetVomsProxyer
		err error
	}{
		{
			v:   &fakeVomsProxy{errors.New("This failed for some reason")},
			err: &NewVomsProxyError{"Could not generate a VOMS proxy"},
		},
		{
			v:   &fakeVomsProxy{err: nil},
			err: nil,
		},
	}

	ctx := context.Background()
	for _, test := range tests {
		_, _, err := NewVomsProxy(ctx, test.v, "")
		if reflect.TypeOf(err) != reflect.TypeOf(test.err) {
			t.Errorf("NewVomsProxy test should have returned %T; got %T instead", test.err, err)
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
			err: os.ErrNotExist,
		},
	}

	for _, test := range tests {
		err := test.v.Remove()
		if reflect.TypeOf(err) != reflect.TypeOf(test.err) {
			t.Errorf("Expected and actual errors do not match.  Expected %T, got %T", test.err, err)
		}
	}
}

func TestCopyToNode(t *testing.T) {
	tests := []struct {
		v   CopyProxyer
		err error
	}{
		{
			v:   &fakeVomsProxy{errors.New("This failed for some reason")},
			err: &CopyProxyError{"Could not copy a VOMS proxy"},
		},
		{
			v:   &fakeVomsProxy{err: nil},
			err: nil,
		},
	}

	ctx := context.Background()
	for _, test := range tests {
		err := CopyToNode(ctx, test.v, "node", "acct", "dest")
		if reflect.TypeOf(err) != reflect.TypeOf(test.err) {
			t.Errorf("NewVomsProxy test should have returned %T; got %T instead", test.err, err)
		}
	}
}
