package proxy

import (
	"fmt"
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
