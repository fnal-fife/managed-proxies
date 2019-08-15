package utils

import (
	"fmt"
	"os/exec"
	"reflect"
	"strconv"
	"strings"

	"github.com/google/shlex"
	"github.com/olekukonko/tablewriter"
	log "github.com/sirupsen/logrus"
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

	log.Debugf("Enumerated args to command are: %s", debugSlice)

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

// DoubleMapToTableSlice takes a map and creates a slice of row slices for use with tablewriters like github.com/olekukonko/tablewriter
// Hopefully deprecate!
func DoubleMapToTableSlice(m map[string]map[string]fmt.Stringer) [][]string {
	table := make([][]string, 0)

	for k, valMap := range m {
		row := []string{k}
		for vKey, vVal := range valMap {
			rowStage := append([]string(nil), row...)
			rowStage = append(rowStage, vKey)
			rowStage = append(rowStage, vVal.String())
			table = append(table, rowStage)
		}
	}

	return table
}

// DoubleErrorMapToTable takes a map[string]map[string]error and generates a table using the provided header slice
func DoubleErrorMapToTable(myMap map[string]map[string]error, header []string) string {
	var b strings.Builder
	data := WrapMapToTableData(myMap)
	table := tablewriter.NewWriter(&b)
	table.SetHeader(header)
	table.AppendBulk(data)
	table.SetBorder(false)
	table.Render()

	return b.String()
}

// WrapMapToTableData wraps MapToTable by taking a map, getting its value, and then passing that to MapToTable with the proper initialization parameters.  This or a function like it should be used by external APIs as opposed to MapToTable.
// func WrapMapToTableData(myMap map[string]map[string]error) [][]string {
func WrapMapToTableData(myObject interface{}) [][]string {
	defer func() {
		if r := recover(); r != nil {
			log.Panicf("Panicked when generating table data, %s", r)
		}
	}()

	v := reflect.ValueOf(myObject)
	return MapToTableData(
		v,
		[][]string{},
		[]string{},
	)
}

// MapToTableData takes an arbitrarily nested map whose value is given in v, iterates through it, and returns each unique key(s)/value set as a row.  Adapted from https://stackoverflow.com/a/53159340
func MapToTableData(v reflect.Value, curData [][]string, curRow []string) [][]string {
	rowStage := append([]string(nil), curRow...)
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		if v.CanInterface() {
			if val, ok := v.Interface().(error); ok {
				rowStage = append(rowStage, val.Error())
				curData = append(curData, rowStage)
				return curData
			}
		}
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Map:
		for _, k := range v.MapKeys() {
			rowStage := append(rowStage, k.String())
			curData = MapToTableData(v.MapIndex(k), curData, rowStage)
		}
	case reflect.Array, reflect.Slice:
		// Empty slice in our structure
		if v.Len() == 0 {
			curData = append(curData, curRow)
			return curData
		}

		for i := 0; i < v.Len(); i++ {
			curData = MapToTableData(v.Index(i), curData, rowStage)
		}
	case reflect.String:
		rowStage = append(rowStage, v.String())
		curData = append(curData, rowStage)
		return curData
	case reflect.Int:
		rowStage = append(rowStage, strconv.FormatInt(v.Int(), 10))
		curData = append(curData, rowStage)
		return curData
	case reflect.Float32:
		rowStage = append(rowStage, strconv.FormatFloat(v.Float(), 'f', -1, 32))
		curData = append(curData, rowStage)
		return curData
	case reflect.Float64:
		rowStage = append(rowStage, strconv.FormatFloat(v.Float(), 'f', -1, 64))
		curData = append(curData, rowStage)
		return curData
	case reflect.Bool:
		rowStage = append(rowStage, strconv.FormatBool(v.Bool()))
		curData = append(curData, rowStage)
		return curData
	default:
		if v.CanInterface() {
			if val, ok := v.Interface().(fmt.Stringer); ok {
				rowStage = append(rowStage, val.String())
				curData = append(curData, rowStage)
			}
		} else {
			curData = append(curData, curRow)
		}
	}
	return curData
}
