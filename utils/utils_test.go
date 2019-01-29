package utils

import (
	"errors"
	"testing"
)

func TestGetArgsFromTemplate(t *testing.T) {
	tests := []struct {
		inString       string
		expectedResult []string
		expectedError  error
	}{
		{
			inString:       "ls -l /tmp/blah",
			expectedResult: []string{"ls", "-l", "/tmp/blah"},
			expectedError:  nil,
		},
		{
			inString:       "echo \"blah\"",
			expectedResult: []string{"echo", "blah"},
			expectedError:  nil,
		},
		{
			inString:       "cmd \"DC=org/THIS IS SOME DN\"",
			expectedResult: []string{"cmd", "DC=org/THIS IS SOME DN"},
			expectedError:  nil,
		},
		{
			inString:       "cmd \\",
			expectedResult: []string{},
			expectedError:  errors.New("Could not split string according to shlex rules: EOF found after escape character"),
		},
	}

	for _, test := range tests {
		result, err := GetArgsFromTemplate(test.inString)
		if errorString(err) != errorString(test.expectedError) {
			t.Errorf("Expected errors did not match:  wanted %s, got %s", test.expectedError, err)
		}

		for i := range result {
			if result[i] != test.expectedResult[i] {
				t.Errorf("Expected results did not match:  wanted %s, got %s", test.expectedResult, result)
			}
		}

	}
}

func TestCheckForExecutables(t *testing.T) {
	tests := []struct {
		exeMap        map[string]string
		expectedError error
	}{
		{
			exeMap: map[string]string{
				"ls": "",
				"cd": "",
			},
			expectedError: nil,
		},
		{
			exeMap: map[string]string{"nowaythisexists": "",
				"cd": "",
			},
			expectedError: errors.New("nowaythisexists was not found in $PATH"),
		},
	}

	for _, test := range tests {
		if err := CheckForExecutables(test.exeMap); errorString(err) != errorString(test.expectedError) {
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
