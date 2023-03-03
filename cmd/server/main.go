package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"oldrosedb"
	"oldrosedb/cmd"
	"os"
	"os/signal"
	"syscall"

	"github.com/pelletier/go-toml"
)

func init() {
	// Print banner.
	banner, _ := ioutil.ReadFile("../../resource/banner.txt")
	fmt.Println(string(banner))
}

// The param config means the config file path for rosedb.
// For the default config file, see config.toml.
//param config 表示rosedb 的配置文件路径
//默认配置文件见 config.toml
var config = flag.String("config", "", "the config file for rosedb")

//参数 dirPath 表示 db 文件和其他配置的持久目录
var dirPath = flag.String("dir_path", "", "the dir path for the database")

func main() {
	flag.Parse()

	// Set the config.
	var cfg oldrosedb.Config
	if *config == "" {
		log.Println("no config set, using the default config.")
		cfg = oldrosedb.DefaultConfig()
	} else {
		c, err := newConfigFromFile(*config)
		if err != nil {
			log.Printf("load config err : %+v\n", err)
			return
		}
		cfg = *c
	}

	if *dirPath == "" {
		log.Println("no dir path set, using the os tmp dir.")
	} else {
		cfg.DirPath = *dirPath
	}

	// Listen the server.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGHUP,
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	db, err := oldrosedb.Open(cfg)
	if err != nil {
		log.Printf("create rosedb err: %+v\n", err)
		return
	}

	server := cmd.NewServerUseDbPtr(db)
	go server.Listen(cfg.Addr)
	<-sig
	server.Stop()
	log.Println("rosedb is ready to exit, bye...")
}

func newConfigFromFile(config string) (*oldrosedb.Config, error) {
	data, err := ioutil.ReadFile(config)
	if err != nil {
		return nil, err
	}

	var cfg = new(oldrosedb.Config)
	err = toml.Unmarshal(data, cfg)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
