package layerinfo

import (
	"fmt"
	"testing"
)

func TestLayerInfoFetcher(t *testing.T) {
	ldf, err := NewLayerInfoFetcher()
	if err != nil {
		t.Errorf("unable to create: %v\n", err)
	}
	l, _ := ldf.Fetch()
	if err != nil {
		t.Errorf("fetch error: %v\n", err)
	}
	fmt.Println(l)

	for _, i := range l {
		t.Logf("layer digest: %s", i.Digest)
		t.Logf("layer size: %d", i.SizeBytes)
	}
	fmt.Printf("total layers: %d\n", len(l))
}

func BenchmarkFetch(b *testing.B) {
	ldf, err := NewLayerInfoFetcher()
	if err != nil {
		b.Logf("unable to create: %v\n", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ldf.Fetch()
	}
}
