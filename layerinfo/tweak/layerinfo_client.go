package main

import (
	"../../layerinfo"
	"fmt"
)

func TestLayerInfoFetcher() {
	ldf, err := layerinfo.NewLayerInfoFetcher()
	if err != nil {
		fmt.Printf("-> unable to create %s: %v\n", ldf, err)
	}
	l, _ := ldf.Fetch()
	if err != nil {
		fmt.Printf("-> fetch error: %v\n", err)
	}

	for _, i := range l {
		fmt.Print("sha256:" + i.Digest + " ")
		fmt.Println(i.SizeBytes)
	}
}

func main() {
	TestLayerInfoFetcher()
}
