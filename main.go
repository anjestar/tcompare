package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"log"
	"strings"
)

type Conn struct {
	Host     string `json:"host"`
	Port     string `json:"port"`
	User     string `json:"user"`
	Pass     string `json:"pass"`
	Database string `json:"db"`
	Table    string `json:"table"`
}

var src = flag.String("src", "", "源表")
var dst = flag.String("dst", "", "目标表")

func main() {
	flag.Parse()
	conf := NewJsonConf().Load("./config.json")
	if *src != "" && *dst != "" {
		conf["src"].Table = *src
		conf["dst"].Table = *dst
	}
	queryRows(conf["src"], nil)
	queryRows(conf["dst"], nil)
}

func queryRows(conn *Conn, where map[string]string) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8",
		conn.User,
		conn.Pass,
		conn.Host,
		conn.Port,
		conn.Database,
	)
	db, err := sql.Open("mysql", dsn)
	CheckErr(err)
	defer db.Close()
	whereStr := ""
	if where != nil {
		for condition, val := range where {
			if strings.IndexAny(val, "<>=!") == 0 {
				whereStr += "AND " + condition
				whereStr += " " + val
			} else {
				whereStr += "AND " + condition + " = " + val
			}
		}
	}
	queryStr := fmt.Sprintf("select * from %s where 1 = 1 %s", conn.Table, whereStr)
	rows, err := db.Query(queryStr)
	CheckErr(err)
	defer rows.Close()
	//fields, _ := rows.Columns()
	result := make([]map[string]string, 0)
	for rows.Next() {
		row := make(map[string]string)
		err = rows.Scan(row...)
		CheckErr(err)
		result = append(result, row)
	}
	fmt.Println(conn.Table, result)
}

func CheckErr(err error) {
	if err != nil {
		panic(err)
	}
}

type JsonConf struct {
}

func NewJsonConf() *JsonConf {
	return &JsonConf{}
}

func (jst *JsonConf) Load(filename string) (conf map[string]*Conn) {
	//ReadFile函数会读取文件的全部内容，并将结果以[]byte类型返回
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatal("读取配置文件失败")
	}
	//读取的数据为json格式，需要进行解码
	err = json.Unmarshal(data, &conf)
	if err != nil {
		log.Fatal("解析配置文件失败")
	}
	return
}
