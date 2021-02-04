package broker

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/fhmq/hmq/logger"
)

type Config struct {
	Worker int    `json:"workerNum"`
	Host   string `json:"host"`
	Port   string `json:"port"`
	Debug  bool   `json:"debug"`
}

type NamedPlugins struct {
	Auth string
}

var DefaultConfig *Config = &Config{
	Worker: 4096,
	Host:   "0.0.0.0",
	Port:   "1883",
}

var (
	log = logger.Prod().Named("broker")
)

func showHelp() {
	fmt.Printf("%s\n", usageStr)
	os.Exit(0)
}

func ConfigureConfig(args []string) (*Config, error) {
	config := &Config{}
	var (
		help       bool
		configFile string
	)
	fs := flag.NewFlagSet("hmq-broker", flag.ExitOnError)
	fs.Usage = showHelp

	fs.BoolVar(&help, "h", false, "Show this message.")
	fs.BoolVar(&help, "help", false, "Show this message.")
	fs.IntVar(&config.Worker, "w", 1024, "worker num to process message, perfer (client num)/10.")
	fs.IntVar(&config.Worker, "worker", 1024, "worker num to process message, perfer (client num)/10.")
	fs.StringVar(&config.Port, "port", "1883", "Port to listen on.")
	fs.StringVar(&config.Port, "p", "1883", "Port to listen on.")
	fs.StringVar(&config.Host, "host", "0.0.0.0", "Network host to listen on")
	fs.StringVar(&configFile, "config", "", "config file for hmq")
	fs.StringVar(&configFile, "c", "", "config file for hmq")
	fs.BoolVar(&config.Debug, "debug", false, "enable Debug logging.")
	fs.BoolVar(&config.Debug, "d", false, "enable Debug logging.")

	fs.Bool("D", true, "enable Debug logging.")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	if help {
		showHelp()
		return nil, nil
	}

	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "D":
			config.Debug = true
		}
	})

	if configFile != "" {
		tmpConfig, e := LoadConfig(configFile)
		if e != nil {
			return nil, e
		} else {
			config = tmpConfig
		}
	}

	if config.Debug {
		log = logger.Debug().Named("broker")
	}

	if err := config.check(); err != nil {
		return nil, err
	}

	return config, nil

}

func LoadConfig(filename string) (*Config, error) {

	content, err := ioutil.ReadFile(filename)
	if err != nil {
		// log.Error("Read config file error: ", zap.Error(err))
		return nil, err
	}
	// log.Info(string(content))

	var config Config
	err = json.Unmarshal(content, &config)
	if err != nil {
		// log.Error("Unmarshal config file error: ", zap.Error(err))
		return nil, err
	}

	return &config, nil
}

func (config *Config) check() error {

	if config.Worker == 0 {
		config.Worker = 1024
	}

	if config.Port != "" {
		if config.Host == "" {
			config.Host = "0.0.0.0"
		}
	}

	return nil
}
