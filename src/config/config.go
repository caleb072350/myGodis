package config

import (
	"bufio"
	"myGodis/src/lib/logger"
	"os"
	"reflect"
	"strconv"
	"strings"
)

type PropertyHolder struct {
	Bind           string   `cfg:"bind"`
	Port           int      `cfg:"port"`
	AppendOnly     bool     `cfg:"appendonly"`
	AppendFilename string   `cfg:"appendfilename"`
	MaxClients     int      `cfg:"maxclients"`
	Peers          []string `cfg:"peers"`
	Self           string   `cfg:"self"`
}

var Properties *PropertyHolder

func LoadConfig(configFilename string) *PropertyHolder {
	config := Properties
	// open config file
	file, err := os.Open(configFilename)
	if err != nil {
		logger.Fatal(err)
		return config
	}
	defer file.Close()

	// read config file
	rawMap := make(map[string]string)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		pivot := strings.IndexAny(line, " ")
		if pivot > 0 && pivot < len(line)-1 { // separator found
			key := line[0:pivot]
			value := strings.Trim(line[pivot+1:], " ")
			rawMap[strings.ToLower(key)] = value
		}
	}

	if err := scanner.Err(); err != nil {
		logger.Fatal(err)
	}

	// parse format
	t := reflect.TypeOf(config)
	v := reflect.ValueOf(config)
	n := t.Elem().NumField()
	for i := 0; i < n; i++ {
		field := t.Elem().Field(i)
		fieldVal := v.Elem().Field(i)
		key, ok := field.Tag.Lookup("cfg")
		if !ok {
			key = field.Name
		}
		value, ok := rawMap[strings.ToLower(key)]
		if ok {
			// fill config
			switch field.Type.Kind() {
			case reflect.String:
				fieldVal.SetString(value)
			case reflect.Int:
				intValue, err := strconv.ParseInt(value, 10, 64)
				if err == nil {
					fieldVal.SetInt(intValue)
				}
			case reflect.Bool:
				boolValue, err := strconv.ParseBool(value)
				if err == nil {
					fieldVal.SetBool(boolValue)
				}
			case reflect.Slice:
				if field.Type.Elem().Kind() == reflect.String {
					slice := strings.Split(value, ",")
					fieldVal.Set(reflect.ValueOf(slice))
				}
			}
		}
	}

	return config
}

func SetupConfig(configFilename string) {
	Properties = LoadConfig(configFilename)
}
