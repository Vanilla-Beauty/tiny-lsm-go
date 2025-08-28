package main

import (
	"fmt"
	"os"
	"tiny-lsm-go/pkg/sst"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s <sst_file_path>\n", os.Args[0])
		os.Exit(1)
	}

	filePath := os.Args[1]

	// 创建SST构建器，块大小为4096字节，启用布隆过滤器
	builder := sst.NewSSTBuilder(4096, true)

	// 添加一些测试数据
	testData := map[string]string{
		"apple":  "red",
		"banana": "yellow",
		"cherry": "red",
		"date":   "brown",
		"grape":  "purple",
		"kiwi":   "green",
		"lemon":  "yellow",
		"orange": "orange",
		"pear":   "yellow",
		"plum":   "purple",
	}

	// 添加数据到构建器
	for key, value := range testData {
		if err := builder.Add(key, value, 1); err != nil {
			fmt.Printf("Error adding key-value pair: %v\n", err)
			os.Exit(1)
		}
	}

	// 构建SST文件
	sstFile, err := builder.Build(1, filePath, nil)
	if err != nil {
		fmt.Printf("Error building SST file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Successfully created SST file: %s\n", filePath)
	fmt.Printf("SST ID: %d\n", sstFile.ID())
	fmt.Printf("First Key: %s\n", sstFile.FirstKey())
	fmt.Printf("Last Key: %s\n", sstFile.LastKey())
	fmt.Printf("Number of Blocks: %d\n", sstFile.NumBlocks())

	sstFile.Close()
}
