package utils

import (
	"errors"
	"fmt"
	"strings"

	"cdcvs.fnal.gov/discompsupp/ken_proxy_push/v3/internal/pkg/utils"
)

// failedPrettifyRolesNodesMap formats a map of failed nodes and roles into node, role columns and appends a message onto the beginning
func failedPrettifyRolesNodesMap(roleNodesMap map[string]map[string]error) string {
	empty := true

	for _, nodeMap := range roleNodesMap {
		if len(nodeMap) > 0 {
			empty = false
			break
		}
	}

	if empty {
		return ""
	}

	table := utils.DoubleErrorMapToTable(roleNodesMap, []string{"Role", "Node", "Error"})

	finalTable := fmt.Sprintf("The following is a list of nodes on which all proxies were not refreshed, and the corresponding roles for those failed proxy refreshes:\n\n%s", table)
	return finalTable

}

// generateNewErrorStringForTable is meant to change an error string based on whether it is currently equal to the defaultError.  If the testError and defaultError match, this func will return an error with the text of errStringSlice.  If they don't, then this func will append the contents of errStringSlice onto the testError text, and return a new error with the combined string.  The separator formats how different error strings should be distinguished.   This func should only be used to concatenate error strings
func generateNewErrorStringForTable(defaultError, testError error, errStringSlice []string, separator string) error {
	var newErrStringSlice []string
	if testError == defaultError {
		newErrStringSlice = errStringSlice
	} else {
		newErrStringSlice = append([]string{testError.Error()}, errStringSlice...)
	}

	return errors.New(strings.Join(newErrStringSlice, separator))
}
