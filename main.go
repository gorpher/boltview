package main

import (
	"bufio"
	"flag"
	"github.com/gorpher/gone"
	"github.com/gorpher/gone/log"
	bolt "go.etcd.io/bbolt"
	"os"
	"strings"
	"time"
)

func main() {
	var dbPath string
	flag.StringVar(&dbPath, "db", "bbolt.db", "bbolt filepath")
	flag.Parse()
	var (
		err error
		db  *bolt.DB
	)
	if !gone.FileExist(dbPath) {
		log.Fatalln(dbPath + " file not  exist")
	}
	db, err = bolt.Open(dbPath, 0644, &bolt.Options{Timeout: time.Second * 5})
	if err != nil {
		log.Fatalln(err)
	}
	reader := bufio.NewReader(os.Stdin)
	fs := Boltview{db: db, currentPath: ""}
	for {
		fs.stdin("")
		linesByte, _, err := reader.ReadLine()
		if err != nil {
			continue
		}
		lines := string(linesByte)
		if strings.TrimSpace(lines) == "" {
			continue
		}
		parseLine(lines, fs.DoCommand)
	}

}
