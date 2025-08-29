package main

import (
	"fmt"
	"log"
	"time"

	"tiny-lsm-go/pkg/common"
	"tiny-lsm-go/pkg/config"
	"tiny-lsm-go/pkg/lsm"
)

func main() {
	cfg := config.DefaultConfig()
	cfg.WAL.CleanInterval = 1 // 1s to clean
	exampleDir := "lsm_example_data"
	db, err := lsm.NewEngine(cfg, exampleDir)

	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Put data
	err = db.Put("key1", "value1")
	if err != nil {
		log.Fatal(err)
	}

	// Get data
	value, found, err := db.Get("key1")
	if err != nil {
		log.Fatal(err)
	}
	if found {
		fmt.Printf("key1: %s\n", string(value))
	}

	// Delete data
	err = db.Delete("key1")
	if err != nil {
		log.Fatal(err)
	}

	// Range iteration
	iter := db.NewIterator()
	defer iter.Close()

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		fmt.Printf("%s: %s\n", string(iter.Key()), string(iter.Value()))
	}

	// Transaction example
	config := lsm.DefaultTransactionConfig()
	config.MaxActiveTxns = 3

	manager := lsm.NewTransactionManager(db, config)

	txn, err := manager.Begin()
	if err != nil {
		log.Fatal(err)
	}

	err = db.PutWithTxn(txn, "k1", "v1")
	if err != nil {
		log.Fatal(err)
	}

	err = db.PutBatchWithTxn(txn, []common.KVPair{
		{Key: "k2", Value: "v2"},
		{Key: "k3", Value: "v3"},
	})
	if err != nil {
		log.Fatal(err)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}

	// check committed data
	for _, key := range []string{"k1", "k2", "k3"} {
		value, found, err := db.Get(key)
		if err != nil {
			log.Fatal(err)
		}
		if found {
			fmt.Printf("found %s: %s\n", key, value)
		} else {
			fmt.Printf("%s: not found\n", key)
		}
	}

	time.Sleep(5 * time.Second) // wait for WAL clean...
	db.Close()
}
