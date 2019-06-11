package utils

import (
	"errors"
	"testing"
)

// TestGetArgsFromTemplate makes sure that GetArgsFromTemplate correctly splits the template string into command arguments
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

// TestCheckForExecutables makes sure that CheckForExecutables correctly finds common executables, and that we get the right error if one is not found
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

func TestMapToTableData(t *testing.T) {
	tests := []struct {
		testObject     interface{}
		expectedResult [][]string
	}{
		{
			testObject:     map[string]string{"key1": "value1", "key2": "value2"},
			expectedResult: [][]string{[]string{"key1", "value1"}, []string{"key2", "value2"}},
		},
		{
			testObject: map[string]map[string]string{
				"expt1": {
					"node1": "error1expt1",
					"node2": "error2expt1",
				},
				"expt2": {
					"node1": "error1expt2",
					"node2": "error2expt2",
				},
				"expt3": {
					"node1": "error1expt3",
				},
			},
			expectedResult: [][]string{
				[]string{"expt1", "node1", "error1expt1"},
				[]string{"expt1", "node2", "error2expt1"},
				[]string{"expt2", "node1", "error1expt2"},
				[]string{"expt2", "node2", "error2expt2"},
				[]string{"expt3", "node1", "error1expt3"},
			},
		},
		{
			testObject: map[string]map[string]error{
				"expt1": {
					"node1": errors.New("error1expt1"),
					"node2": errors.New("error2expt1"),
				},
				"expt2": {
					"node1": errors.New("error1expt2"),
					"node2": errors.New("error2expt2"),
				},
				"expt3": {
					"node1": errors.New("error1expt3"),
				},
			},
			expectedResult: [][]string{
				[]string{"expt1", "node1", "error1expt1"},
				[]string{"expt1", "node2", "error2expt1"},
				[]string{"expt2", "node1", "error1expt2"},
				[]string{"expt2", "node2", "error2expt2"},
				[]string{"expt3", "node1", "error1expt3"},
			},
		},
		{
			testObject: map[string]map[string]int{
				"expt1": {
					"node1": 1,
					"node2": 2,
				},
				"expt2": {
					"node1": 3,
					"node2": 4,
				},
				"expt3": {
					"node1": 5,
				},
			},
			expectedResult: [][]string{
				[]string{"expt1", "node1", "1"},
				[]string{"expt1", "node2", "2"},
				[]string{"expt2", "node1", "3"},
				[]string{"expt2", "node2", "4"},
				[]string{"expt3", "node1", "5"},
			},
		},
	}

	for _, test := range tests {
		if testVal := WrapMapToTableData(test.testObject); checkUnorderedSliceSliceStringEqual(testVal, test.expectedResult) {
			t.Errorf("Expected Results did not match:  expected %v, got %v", test.expectedResult, testVal)
		}
	}
}

// Adapted from https://stackoverflow.com/a/15312097
func checkUnorderedSliceSliceStringEqual(a, b [][]string) bool {
	// If one is nil, the other must also be nil.
	if (a == nil) != (b == nil) {
		return false
	}

	if len(a) != len(b) {
		return false
	}

	// We don't care about order.  So just pop elements out as we find them in both slices
	// Copy slices so we don't accidentally affect the slices passed in (since slices are pointers)
	copya := append([][]string(nil), a...)
	copyb := append([][]string(nil), b...)
	for len(copya) > 0 {
		testResult := false
		testVal := copya[0]
		for j := range copyb {
			if checkUnorderedSliceStringEqual(testVal, copyb[j]) {
				copya = copya[1:]
				copyb = append(copyb[:j], copyb[j+1:]...)
				testResult = true
				break
			}
		}
		if !testResult {
			return false
		}
	}

	return true
}

// Adapted from https://stackoverflow.com/a/15312097
func checkUnorderedSliceStringEqual(a, b []string) bool {
	// If one is nil, the other must also be nil.
	if (a == nil) != (b == nil) {
		return false
	}

	if len(a) != len(b) {
		return false
	}

	// We don't care about order.  So just pop elements out as we find them in both slices
	// Copy slices so we don't accidentally affect the slices passed in (since slices are pointers)
	copya := append([]string(nil), a...)
	copyb := append([]string(nil), b...)
	for len(a) > 0 {
		testResult := false
		testValue := copya[0]
		for j := range copyb {
			if testValue == b[j] {
				copya = copya[1:]
				copyb = append(copyb[:j], copyb[j+1:]...)
				testResult = true
				break
			}
		}
		if !testResult {
			return false
		}
	}

	return true
}
