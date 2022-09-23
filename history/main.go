package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
)

// SIMPLE BENCHMARK
// duration				n transfers(pagination)
// --------------------------------------------
// 4s (1m43)			1K
// 41s (14m)			10K
// 6m23s (2h30m)		100K
// 1h36m15s	(23h)		1M
// 30h (21d) 			22M

// BQ client timeout?

type TransferQuery struct {
	Row            int     `json:"row"`
	Page           int     `json:"page"`
	Addresss       *string `json:"address"`
	ExtrinsicIndex string  `json:"extrinsic_index"`
	FromBlock      int     `json:"from_block"`
	ToBlock        int     `json:"to_block"`
	Direction      *string `json:"direction"`
	IncludeTotal   *bool   `json:"include_total"`
	AssetSymbol    *string `json:"asset_symbol"`
}

type TransferBody struct {
	ExtrinsicIndex string
	BlockNum       int
	BlockTimestamp int
	From           string
	To             string
	Amount         string
	Success        bool
}

func (t *TransferBody) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"extrinsic_index": t.ExtrinsicIndex,
		"block_num":       t.BlockNum,
		"block_timestamp": t.BlockTimestamp,
		"from":            t.From,
		"to":              t.To,
		"amount":          t.Amount,
		"success":         t.Success,
	}, bigquery.NoDedupeID, nil
}

func doNodlRequest(rows, page int) []*TransferBody {
	q := TransferQuery{
		Row:  rows,
		Page: page,
		// FromBlock: 304864,  // first block within parachain
		// ToBlock:   1040266, // last block known
	}

	jsonData, err := json.Marshal(q)
	if err != nil {
		log.Fatal(err)
	}

	req, err := http.NewRequest("POST", "https://nodle.api.subscan.io/api/scan/transfers", bytes.NewBuffer(jsonData))
	if err != nil {
		fmt.Print(err.Error())
		os.Exit(1)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Add("x-api-key", "abc")

	client := &http.Client{}
	response, err := client.Do(req)
	if err != nil {
		fmt.Println("Errored when sending request to the server")
		return nil
	}

	responseData, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Fatal(err)
	}

	var f interface{}
	json.Unmarshal(responseData, &f)

	fmt.Println(string(responseData))
	m := f.(map[string]interface{})

	foomap := m["data"]
	v := foomap.(map[string]interface{})
	inter := v["transfers"]
	if inter == nil {
		return nil
	}

	sli := inter.([]interface{})

	var transfers []*TransferBody
	for _, e := range sli {
		a := e.(map[string]interface{})
		blockNum := int(a["block_num"].(float64))
		blockTimestamp := int(a["block_timestamp"].(float64))

		transfers = append(transfers, &TransferBody{
			ExtrinsicIndex: a["extrinsic_index"].(string),
			BlockNum:       blockNum,
			BlockTimestamp: blockTimestamp,
			From:           a["from"].(string),
			To:             a["to"].(string),
			Amount:         a["amount"].(string),
			Success:        a["success"].(bool),
		})

	}
	return transfers
}

func main() {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, "abc")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// pageLimit := 10
	rows := 10
	// maxSize := 100000
	var t []*TransferBody

	start := time.Now()

	doNodlRequest(rows, 0)

	// for i := 0; i < pageLimit; i++ {
	// 	t = append(t, doNodlRequest(rows, i)...)
	// 	if t == nil {
	// 		break
	// 	}

	// 	if len(t) >= maxSize {
	// 		t = nil
	// 		inserter := client.Dataset("abc").Table("abc").Inserter()

	// 		if err := inserter.Put(ctx, t); err != nil {
	// 			log.Fatal(err)
	// 		}
	// 	}
	// }

	elapsed := time.Since(start)
	log.Printf("%d pages transfers took %s", len(t), elapsed)
}
