package main

import (
	"fmt"
	_ "net"
	"os"
	"sync"
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

var wg sync.WaitGroup

func startTimer(sql string, frequency uint) {
	for {
		fmt.Println(sql)
		//now := time.Now()
		//next := now.Add(time.Second * time.Duration(frequency))
		t := time.NewTimer(time.Second * time.Duration(frequency))
		<-t.C
	}

}

func loop() {
	for _, s := range Sqls.Sql {
		wg.Add(1)
		go startTimer(s.Sql, s.Frequency)
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
	loop()
	wg.Wait()
}
