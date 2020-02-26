package notifications

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type fakeSender struct {
	err error
}

func (f *fakeSender) SendMessage(ctx context.Context, msg string, ConfigInfo map[string]string) error {
	return f.err
}

func TestSendMessage(t *testing.T) {
	tests := []struct {
		s   SendMessager
		err error
	}{
		{
			s:   &fakeSender{errors.New("This failed for some reason")},
			err: &SendMessageError{"Could not get a new grid proxy from gridProxyer"},
		},
		{
			s:   &fakeSender{err: nil},
			err: nil,
		},
	}

	ctx := context.Background()
	var c map[string]string

	for _, test := range tests {
		err := SendMessage(ctx, test.s, "", c)
		if reflect.TypeOf(err) != reflect.TypeOf(test.err) {
			t.Errorf("SendMessage test should have returned %T; got %T instead", test.err, err)
		}
	}

}
