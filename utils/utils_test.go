package utils

import (
	"errors"
	"strconv"
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
	// Moved tests out of this func to make the code lookc cleaner.

	for _, test := range mapTableTests {
		if testVal := WrapMapToTableData(test.testObject); !checkUnorderedSliceSliceStringEqual(testVal, test.expectedResult) {
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
			if sameStringSliceUnordered(testVal, copyb[j]) {
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

// From https://stackoverflow.com/a/36000696
func sameStringSliceUnordered(x, y []string) bool {
	if len(x) != len(y) {
		return false
	}

	diff := make(map[string]int, len(x))

	// Create a map of string --> int.
	for _, _x := range x {
		// Set value for each x-key to however many times it appears in x
		diff[_x]++
	}

	for _, _y := range y {
		// If the string _y is not in diff, return false
		if _, ok := diff[_y]; !ok {
			return false
		}
		// If _y is in diff, decrement it
		diff[_y]--

		// If _y appears in x and y the same number of times, delete diff[_y]
		if diff[_y] == 0 {
			delete(diff, _y)
		}
	}

	if len(diff) == 0 {
		return true
	}

	return false
}

type myStringerType struct{ value int }

func (m myStringerType) String() string {
	return strconv.Itoa(m.value)
}

type myNotStringerType struct{ value int }

// There are so many tests here that I just moved it out of the func
var mapTableTests = []struct {
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
	{
		testObject: map[string]map[string]float64{
			"expt1": {
				"node1": 1.1,
				"node2": 2.1,
			},
			"expt2": {
				"node1": 3.1,
				"node2": 4.1,
			},
			"expt3": {
				"node1": 5.1,
			},
		},
		expectedResult: [][]string{
			[]string{"expt1", "node1", "1.1"},
			[]string{"expt1", "node2", "2.1"},
			[]string{"expt2", "node1", "3.1"},
			[]string{"expt2", "node2", "4.1"},
			[]string{"expt3", "node1", "5.1"},
		},
	},
	{
		testObject: map[string]map[string]myStringerType{
			"expt1": {
				"node1": myStringerType{1},
				"node2": myStringerType{2},
			},
			"expt2": {
				"node1": myStringerType{3},
				"node2": myStringerType{4},
			},
			"expt3": {
				"node1": myStringerType{5},
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
	{
		testObject: map[string]map[string]bool{
			"expt1": {
				"node1": true,
				"node2": false,
			},
			"expt2": {
				"node1": false,
				"node2": true,
			},
			"expt3": {
				"node1": true,
			},
		},
		expectedResult: [][]string{
			[]string{"expt1", "node1", "true"},
			[]string{"expt1", "node2", "false"},
			[]string{"expt2", "node1", "false"},
			[]string{"expt2", "node2", "true"},
			[]string{"expt3", "node1", "true"},
		},
	},
	{
		testObject: map[string][]string{
			"expt1": []string{
				"node1",
				"node2",
			},
			"expt2": []string{
				"node1",
				"node2",
			},
			"expt3": []string{},
		},
		expectedResult: [][]string{
			[]string{"expt1", "node1"},
			[]string{"expt1", "node2"},
			[]string{"expt2", "node1"},
			[]string{"expt2", "node2"},
			[]string{"expt3"},
		},
	},
	{
		testObject: map[string]int{
			"expt1": 1,
			"expt2": 2,
			"expt3": 3,
		},
		expectedResult: [][]string{
			[]string{"expt1", "1"},
			[]string{"expt2", "2"},
			[]string{"expt3", "3"},
		},
	},
	{
		testObject: []string{
			"expt1",
			"expt2",
			"expt3",
		},
		expectedResult: [][]string{
			[]string{"expt1"},
			[]string{"expt2"},
			[]string{"expt3"},
		},
	},
	{
		testObject: map[string]myNotStringerType{
			"expt1": myNotStringerType{1},
			"expt2": myNotStringerType{2},
			"expt3": myNotStringerType{3},
		},
		expectedResult: [][]string{},
	},
	{
		testObject:     myNotStringerType{1},
		expectedResult: [][]string{},
	},
	{
		testObject: []string{
			"expt1",
			"expt2",
			"expt3",
		},
		expectedResult: [][]string{
			[]string{"expt1"},
			[]string{"expt2"},
			[]string{"expt3"},
		},
	},
	{
		testObject: myStringerType{1},
		expectedResult: [][]string{
			[]string{"1"},
		},
	},
	{
		testObject: 1,
		expectedResult: [][]string{
			[]string{"1"},
		},
	},
}
