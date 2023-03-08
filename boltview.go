package main

import (
	"fmt"
	bolt "go.etcd.io/bbolt"
	"os"
	"path"
	"strings"
)

type Boltview struct {
	db          *bolt.DB
	currentPath string
	bucketName  string
}

func (f *Boltview) isRoot() bool {
	return f.currentPath == ""
}

func (f *Boltview) getPath(name string) string {
	if name == "" {
		return f.currentPath
	}
	if !strings.HasPrefix(name, "/") {
		name = f.currentPath + "/" + name
	}
	return strings.Trim(name, "/")
}

func (f *Boltview) getRootBucket(name string, createIfMissing bool, tx *bolt.Tx) (bucket *bolt.Bucket) {
	if createIfMissing {
		bucket, _ = tx.CreateBucketIfNotExists([]byte(name))
		return bucket
	}
	bucket = tx.Bucket([]byte(name))
	return bucket
}

func (f *Boltview) DoCommand(command string, args ...string) {
	switch command {
	case "cat":
		f.Cat(args)
		return
	case "ls":
		f.List(args)
		return
	case "cd":
		f.CD(args)
		return
	case "pwd":
		f.Pwd()
		return
	case "touch":
		f.Touch(args)
		return
	case "mkdir":
		f.Mkdir(args)
		return
	case "rm":
		f.Rm(args)
		return
	case "write":
		f.Write(args)
		return
	case "help":
		printHelp()
	case "exit":
		os.Exit(0)
	default:
		fmt.Println("invalid command")
	}
}

func (f *Boltview) back() {
	entries := strings.Split(f.currentPath, "/")
	if len(entries) > 0 {
		f.currentPath = strings.Join(entries[:len(entries)-1], "/")
		return
	}
	f.currentPath = ""
}

func (f *Boltview) cd(path string) {
	f.db.View(func(tx *bolt.Tx) error {
		bucketName, bucket := f.rootBucket(path, tx)
		if bucket == nil {
			fmt.Println("directory not exist")
			return nil
		}
		subPath := strings.TrimPrefix(path, bucketName)
		if subPath == "" {
			f.currentPath = strings.TrimPrefix(path, "/")
			return nil
		}
		bucket = getBucket(subPath, false, bucket)
		if bucket != nil {
			f.currentPath = strings.TrimPrefix(path, "/")
			return nil
		}
		fmt.Println("directory not exist")
		return nil
	})
}

func (f *Boltview) List(args []string) {
	var name string
	if len(args) != 0 {
		name = args[0]
	}
	name = f.getPath(name)
	tx, err := f.db.Begin(false)
	if err != nil {
		return
	}
	defer tx.Commit()
	infos := ls(name, tx)
	for _, info := range infos {
		if info.isBucket {
			fmt.Printf("%s/ (total:%d)\n", info.name, info.total)
		} else {
			fmt.Printf("%s (size:%d)\n", info.name, info.size)
		}
	}
}

func (f *Boltview) Rm(args []string) {
	if len(args) == 0 || args[0] == "" {
		return
	}
	name := f.getPath(args[0])
	f.db.Update(func(tx *bolt.Tx) error {
		bucketName, bucket := f.rootBucket(name, tx)
		if bucket == nil {
			return nil
		}
		subPath := strings.TrimPrefix(name, bucketName)
		subPath = strings.Trim(subPath, "/")
		if subPath == "" {
			tx.DeleteBucket([]byte(bucketName))
			return nil
		}
		var fileName string
		subPath, fileName = path.Split(subPath)
		if subPath != "" {
			bucket = getBucket(subPath, false, bucket)
			if bucket == nil {
				fmt.Println("directory not exist")
				return nil
			}
		}
		if fileName != "" {
			bucket.Delete([]byte(fileName))
			return nil
		}
		return nil
	})

}

func (f *Boltview) Mkdir(args []string) {
	if len(args) == 0 {
		return
	}
	name := f.getPath(args[0])
	f.db.Update(func(tx *bolt.Tx) error {
		bucketName, bucket := f.rootBucket(name, tx)
		if bucket == nil && bucketName != "" {
			tx.CreateBucketIfNotExists([]byte(bucketName))
		}
		subPath := strings.TrimPrefix(name, bucketName)
		subPath = strings.Trim(subPath, "/")
		if subPath == "" {
			return nil
		}
		var fileName string
		subPath, fileName = path.Split(subPath)
		subPath = strings.Trim(subPath, "/")
		if subPath == "" {
			bucket.CreateBucketIfNotExists([]byte(fileName))
			return nil
		}
		bucket = getBucket(subPath, true, bucket)
		if bucket == nil {
			fmt.Println("directory not exist")
			return nil
		}
		if fileName != "" {
			bucket.CreateBucketIfNotExists([]byte(fileName))
			return nil
		}
		return nil
	})
}

func (f *Boltview) Touch(args []string) {
	if len(args) == 0 {
		return
	}
	name := args[0]
	if name == "" {
		return
	}
	name = f.getPath(args[0])
	f.db.Update(func(tx *bolt.Tx) error {
		bucketName, bucket := f.rootBucket(name, tx)
		if bucket == nil {
			fmt.Println("directory not exist")
			return nil
		}
		subPath := strings.TrimPrefix(name, bucketName)
		subPath = strings.Trim(subPath, "/")
		if subPath == "" {
			return nil
		}
		var fileName string
		subPath, fileName = path.Split(subPath)
		if subPath != "" {
			bucket = getBucket(subPath, false, bucket)
			if bucket == nil {
				fmt.Println("directory not exist")
				return nil
			}
		}
		bucket.Put([]byte(fileName), []byte(""))
		return nil
	})

}

func (f *Boltview) Cat(args []string) {
	if len(args) == 0 {
		return
	}
	name := args[0]
	if name == "" {
		return
	}
	name = f.getPath(args[0])
	tx, err := f.db.Begin(false)
	if err != nil {
		return
	}
	defer tx.Commit()
	bucketName, bucket := f.rootBucket(name, tx)
	if bucket == nil {
		fmt.Println("directory not exist")
		return
	}
	subPath := strings.TrimPrefix(name, bucketName)
	subPath = strings.Trim(subPath, "/")
	if subPath == "" {
		return
	}
	var fileName string
	subPath, fileName = path.Split(subPath)
	subPath = strings.Trim(subPath, "/")
	if subPath != "" {
		bucket = getBucket(subPath, false, bucket)
		if bucket == nil {
			fmt.Println("directory not exist")
			return
		}
	}
	value := bucket.Get([]byte(fileName))
	fmt.Println(string(value))
}

func (f *Boltview) Pwd() {
	fmt.Printf("%s\n", f.currentPath)
}

func (f *Boltview) Write(args []string) {
	if len(args) < 2 || args[0] == "" {
		return
	}
	name := f.getPath(args[0])
	data := args[1]
	f.db.Update(func(tx *bolt.Tx) error {
		bucketName, bucket := f.rootBucket(name, tx)
		if bucket == nil {
			fmt.Println("directory not exist")
			return nil
		}
		subPath := strings.TrimPrefix(name, bucketName)
		subPath = strings.Trim(subPath, "/")
		if subPath == "" {
			return nil
		}
		var fileName string
		subPath, fileName = path.Split(subPath)
		if subPath != "" {
			bucket = getBucket(subPath, false, bucket)
			if bucket == nil {
				fmt.Println("directory not exist")
				return nil
			}
		}
		bucket.Put([]byte(fileName), []byte(data))
		return nil

	})

}

func (f *Boltview) rootBucket(name string, tx *bolt.Tx) (string, *bolt.Bucket) {
	absPath := f.cleanPath(name)
	slash := strings.IndexRune(absPath, '/')
	if slash < 0 {
		return absPath, tx.Bucket([]byte(absPath))
	}
	return absPath[:slash], tx.Bucket([]byte(absPath[:slash]))
}

func (f *Boltview) cleanPath(name string) string {
	if name == "" {
		return f.currentPath
	}
	if strings.HasPrefix(name, "/") {
		return strings.TrimPrefix(name, "/")
	}
	return path.Join(f.currentPath, name)
}

func (f *Boltview) CD(args []string) {
	if len(args) == 0 {
		return
	}
	var target = args[0]
	if target == "" || target == "." {
		return
	}
	if target == ".." {
		f.back()
		return
	}
	if !f.isRoot() {
		target = f.currentPath + "/" + target
	}
	f.cd(target)
}

func (f *Boltview) stdin(line string) {
	fmt.Printf("bolt:/%s# %s", f.currentPath, line)
}

func parseLine(line string, doFunc func(string, ...string)) {
	if len(line) == 0 {
		return
	}
	arr := strings.Split(strings.TrimSpace(line), " ")
	command := arr[0]
	args := arr[1:]
	//if !(command == "ls" || command == "CD" || command == "cat" || command == "exit") {
	//	fmt.Println("invalid command")
	//	return
	//}
	if command == "CD" && len(args) > 1 {
		fmt.Println("CD: too many arguments")
		return
	}
	if command == "cat" && len(args) > 1 {
		fmt.Println("cat: arguments invalid")
		return
	}
	if command == "ls" && len(args) > 1 {
		fmt.Println("ls: too many arguments")
		return
	}

	if len(arr) >= 2 {
		doFunc(command, arr[1:]...)
	} else {
		doFunc(command)
	}
}

func splitBucket(absPath string) (bucket, bucketPath string) {
	slash := strings.IndexRune(absPath, '/')
	// Bucket but no cleanPath
	if slash < 0 {
		return absPath, ""
	}
	return absPath[:slash], absPath[slash+1:]

}

func getBucket(subPath string, ifNotExistCreate bool, bucket *bolt.Bucket) *bolt.Bucket {
	subPath = strings.Trim(subPath, "/")
	entries := strings.Split(subPath, "/")
	for _, entry := range entries {
		if entry == "" {
			return nil
		}
		if ifNotExistCreate {
			bucket, _ = bucket.CreateBucketIfNotExists([]byte(entry))
		} else {
			bucket = bucket.Bucket([]byte(entry))
		}
		if bucket == nil {
			return nil
		}
	}
	return bucket
}

func ls(name string, tx *bolt.Tx) []FileInfo {
	var list []FileInfo
	if name == "" {
		tx.ForEach(func(k []byte, b *bolt.Bucket) error {
			stats := b.Stats()
			list = append(list, FileInfo{isBucket: true, total: stats.KeyN, name: string(k)})
			return nil
		})
		return list
	}
	bucketName, bucketPath := splitBucket(name)
	if bucketName == "" {
		return nil
	}
	bucket := tx.Bucket([]byte(bucketName))
	if bucket == nil {
		return nil
	}
	if bucketPath == "" {
		bucket.ForEachBucket(func(k []byte) error {
			stats := bucket.Bucket(k).Stats()
			list = append(list, FileInfo{isBucket: true, total: stats.KeyN, name: string(k)})
			return nil
		})
		bucket.ForEach(func(k, v []byte) error {
			if v != nil {
				list = append(list, FileInfo{name: string(k), size: len(v)})
			}
			return nil
		})
		return list
	}
	bucket = getBucket(bucketPath, false, bucket)
	if bucket == nil {
		return list
	}

	bucket.ForEachBucket(func(k []byte) error {
		stats := bucket.Bucket(k).Stats()
		list = append(list, FileInfo{isBucket: true, total: stats.KeyN, name: string(k)})
		return nil
	})
	bucket.ForEach(func(k, v []byte) error {
		if v != nil {
			list = append(list, FileInfo{name: string(k), size: len(v)})
		}
		return nil
	})
	return list
}

func printHelp() {
	fmt.Println("command: cat ls cd pwd touch mkdir rm write exit")
}

type FileInfo struct {
	isBucket bool
	total    int
	name     string
	size     int
}
