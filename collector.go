package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jinzhu/configor"

	"database/sql"
	_ "github.com/go-sql-driver/mysql"

	"github.com/howeyc/fsnotify"
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

type collectItem struct {
	Table     string
	Sql       string
	Frequency uint
	Delay     uint
}

var Sqls = struct {
	Sql []collectItem
}{}

var Ctx = struct {
	MysqlConn *sql.DB
	Wg        sync.WaitGroup
	TcpConn   net.Conn
}{}

var lock sync.Mutex

/**
 ************ MySQL *************
 * {{{
 */
func initMySQL() error {
	var err error = nil
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		Config.DB.Username,
		Config.DB.Password,
		Config.DB.Host,
		Config.DB.Port,
	)
	Ctx.MysqlConn, err = sql.Open("mysql", connStr)
	if err != nil {
		return err
	}

	err = Ctx.MysqlConn.Ping()
	if err != nil {
		return err
	}
	return nil
}

func formatSql(s string, frequency, delay uint) string {
	endTime := time.Unix(int64(time.Now().Unix())-int64(delay), 0)
	startTime := time.Unix(int64(time.Now().Unix()-int64(frequency)-int64(delay)), 0)
	end_time := endTime.Format("2006-01-02 15:04:05")
	start_time := startTime.Format("2006-01-02 15:04:05")
	one_week_duration, _ := time.ParseDuration("-168h")
	last_week_end_time := endTime.Add(one_week_duration).Format("2006-01-02 15:04:05")
	last_week_start_time := startTime.Add(one_week_duration).Format("2006-01-02 15:04:05")
	s = strings.Replace(s, "{START_TIME}", start_time, -1)
	s = strings.Replace(s, "{END_TIME}", end_time, -1)
	s = strings.Replace(s, "{LAST_WEEK_START_TIME}", last_week_start_time, -1)
	s = strings.Replace(s, "{LAST_WEEK_END_TIME}", last_week_end_time, -1)
	return s
}

/**
 * }}}
 ************ MySQL *************
 */

/**
 ************ Telegraf *************
 * {{{
 */

func initTcpConn() error {
	var err error = nil
	Ctx.TcpConn, err = net.Dial("tcp", fmt.Sprintf("%s:%d", Config.Server.Host, Config.Server.Port))
	if err != nil {
		return err
	}
	return nil
}

func send(msg string) error {
	if len(msg) < 1 {
		return nil
	}
	if msg[len(msg)-1] != '\n' {
		msg += "\n"
	}
	_, err := Ctx.TcpConn.Write([]byte(msg))
	if err != nil {
		return err
	}
	return nil
}

/**
 * }}}
 ************ Telegraf *************
 */

func executeQuery(s, table string) {
	rows, err := Ctx.MysqlConn.Query(s)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Query error ", err.Error())
		return
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Get columns error ", err.Error())
		return
	}

	values := make([]sql.RawBytes, len(columns))

	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	if rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
		}

		var value string
		msg := fmt.Sprintf("%s ", table)
		for i, col := range values {
			if i > 0 {
				msg += ","
			}
			if col == nil {
				value = "NULL"
			} else {
				value = string(col)
			}
			msg += columns[i] + "=" + value
		}

		err := send(msg)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
		}
	}

}

func startTimer(item *collectItem) {
	for {
		lock.Lock()
		lock.Unlock()

		newsql := formatSql(item.Sql, item.Frequency, item.Delay)

		fmt.Println(newsql)
		executeQuery(newsql, item.Table)
		t := time.NewTimer(time.Second * time.Duration(item.Frequency))
		<-t.C
	}

}

func loop() {
	for k, _ := range Sqls.Sql {
		Ctx.Wg.Add(1)
		go startTimer(&Sqls.Sql[k])
	}
}

func watchSqlJson() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		os.Exit(1)
	}

	go func() {
		for {
			select {
			case ev := <-watcher.Event:
				if ev.IsModify() {
					loadSqlJson()
					fmt.Println("Sql File reloaded")
					err = watcher.Watch("./sql.json")
					if err != nil {
						os.Exit(1)
					}
				}
			case err := <-watcher.Error:
				fmt.Println(err)
			}
		}
	}()

	err = watcher.Watch("./sql.json")
	if err != nil {
		os.Exit(1)
	}
}

func loadSqlJson() {
	lock.Lock()
	if err := configor.Load(&Sqls, "sql.json"); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to load sql.json", err.Error())
		os.Exit(1)
	}
	lock.Unlock()
}

func main() {
	if err := configor.Load(&Config, "config.json"); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to load config.json")
		os.Exit(1)
	}

	loadSqlJson()
	go watchSqlJson()

	if len(Sqls.Sql) == 0 {
		fmt.Fprintln(os.Stdout, "No task to work")
		os.Exit(0)
	}

	if err := initMySQL(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed initialize MySQL ", err.Error())
		os.Exit(1)
	}

	if err := initTcpConn(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect telegraf", err.Error())
		os.Exit(1)
	}
	defer func() {
		Ctx.MysqlConn.Close()
		Ctx.TcpConn.Close()
	}()

	loop()
	Ctx.Wg.Wait()
}
