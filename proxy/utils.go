package proxy

import (
	"fmt"
	"os/exec"
	"strconv"

	"github.com/google/shlex"
)

func getArgsFromTemplate(s string) ([]string, error) {
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

func checkForExecutables(exeMap map[string]string) error {
	// Make sure our required executables are in $PATH
	for exe := range exeMap {
		if pth, err := exec.LookPath(exe); err != nil {
			return fmt.Errorf("%s was not found in $PATH.", exe)
		} else {
			exeMap[exe] = pth
		}
	}
	return nil
}
