package proxy

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"

	"github.com/google/shlex"
)

func getArgsFromTemplate(s string) ([]string, error) {
	args := make([]string, 0)
	args, err := shlex.Split(s)
	if err != nil {
		fmt.Println("Could not split string according to shlex rules")
		return []string{}, err
	}

	debugSlice := make([]string, 0)
	for num, f := range args {
		debugSlice = append(debugSlice, strconv.Itoa(num), f)
	}
	//TODO:  This becomes a debug logging statement
	fmt.Println("Enumerated args to command are: ", debugSlice)
	return args, nil
}

func checkForExecutables(exeMap map[string]string) {
	// Make sure our required executables are in $PATH
	for exe := range exeMap {
		if pth, err := exec.LookPath(exe); err != nil {
			fmt.Printf("%s was not found in $PATH.  Exiting", exe)
			os.Exit(1)
		} else {
			exeMap[exe] = pth
		}
	}
}
