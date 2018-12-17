package utils

import (
	"fmt"
	"os/exec"
	"strconv"

	"github.com/google/shlex"
)

// GetArgsFromTemplate takes a template string and breaks it into a slice of args
func GetArgsFromTemplate(s string) ([]string, error) {
	args := make([]string, 0)
	args, err := shlex.Split(s)
	if err != nil {
		return []string{}, fmt.Errorf("Could not split string according to shlex rules: %s", err)
	}

	debugSlice := make([]string, 0)
	for num, f := range args {
		debugSlice = append(debugSlice, strconv.Itoa(num), f)
	}

	fmt.Println("Enumerated args to command are: ", debugSlice)

	return args, nil
}

// CheckForExecutables takes a map of executables of the form {"name_of_executable": "whatever"} and
// checks if each executable is in $PATH.  If so, it saves the path in the map.  If not, it returns an error
func CheckForExecutables(exeMap map[string]string) error {
	for exe := range exeMap {
		if pth, err := exec.LookPath(exe); err != nil {
			return fmt.Errorf("%s was not found in $PATH", exe)
		} else {
			exeMap[exe] = pth
		}
	}
	return nil
}
