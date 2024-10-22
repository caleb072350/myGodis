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
	Bind           string `cfg:"bind"`
	Port           int    `cfg:"port"`
	AppendOnly     bool   `cfg:"appendonly"`
	AppendFilename string `cfg:"appendfilename"`
	MaxClients     int    `cfg:"maxclients"`
}

var Properties *PropertyHolder

func LoadConfig(configFilename string) *PropertyHolder {
	// open config file
	file, err := os.Open(configFilename)
	if err != nil {
		logger.Fatal(err)
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
		parts := strings.Split(line, "=")
		if len(parts) != 2 {
			logger.Fatal("invalid config: " + line)
		}
		rawMap[strings.ToLower(parts[0])] = parts[1]
	}

	if err := scanner.Err(); err != nil {
		logger.Fatal(err)
	}

	// parse format
	config := &PropertyHolder{}
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
			}
		}
	}

	return config
}

func SetupConfig(configFilename string) {
	Properties = LoadConfig(configFilename)
}
