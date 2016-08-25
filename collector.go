package main

import (
	"fmt"
	_ "net"
	"os"
	"time"

	_ "github.com/influxdata/telegraf/plugins/parsers"
	"github.com/jinzhu/configor"

	_ "github.com/go-sql-driver/mysql"
)

var Config = struct {
	Server struct {
		Host string `default:"127.0.0.1"`
		Port uint   `default:8194`
	}

	DB struct {
		Host     string `default:"127.0.0.1"`
		Port     uint   `default:3306`
		Username string
		Password string
	}
}{}

var Sqls = struct {
	Sql []struct {
		Sql       string
		Frequency uint
	}
}{}

func executeSql(sql string, timer *time.Timer) {
	<-timer.C
	fmt.Println(sql)
}

func loop() {
	timers := make([]*time.Timer, len(Sqls.Sql))

	for k, s := range Sqls.Sql {
		timers[k] = time.NewTimer(time.Second * time.Duration(s.Frequency))
		go executeSql(s.Sql, timers[k])
	}

	for {
		select {}
	}
}

func main() {
	if err := configor.Load(&Config, "config.json"); err != nil {
		fmt.Fprint(os.Stderr, "Failed to load config.json")
		os.Exit(1)
	}

	if err := configor.Load(&Sqls, "sql.json"); err != nil {
		fmt.Fprint(os.Stderr, "Failed to load sql.json", err.Error())
		os.Exit(1)
	}

	if len(Sqls.Sql) == 0 {
		fmt.Fprintln(os.Stdout, "No task to work")
		os.Exit(0)
	}
	go loop()
}
