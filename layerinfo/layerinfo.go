package layerinfo

import (
	"errors"
	"io/ioutil"
	"strconv"

	"k8s.io/api/core/v1"
	"fmt"
)

const (
	dockerDir = "/var/lib/docker"
	dbDir     = dockerDir + "/image"
	// (dbDir) + driverType + ..; dist files
	distDir    = "/distribution"
	diffidDir  = distDir + "/diffid-by-digest/sha256"
	layerdbDir = "/layerdb/sha256"
	diffidFile = "/diff"
	sizeFile   = "/size"
)

var (
	driverTypes = map[string]func(*LayerInfoFetcher) ([]*v1.Layer, error){
		"overlay2": (*LayerInfoFetcher).fetchOverlayFs2,
		//"overlay":  (*LayerInfoFetcher).fetchOverlayFs,
		//"aufs":     (*LayerInfoFetcher).fetchAufs,
	}
)

type LayerInfoFetcher struct {
	driverType   string
	layerInfoMap map[string]*v1.Layer
}

func GetFsDriver() (string, error) {
	files, err := ioutil.ReadDir(dockerDir)
	if err != nil {
		return "", errors.New("fetcher: no docker dir found")
	}

	for _, f := range files {
		if _, ok := driverTypes[f.Name()]; ok {
			return f.Name(), nil
		}
	}
	return "", errors.New("fetcher: no storage driver implemented")
}

func NewLayerInfoFetcher() (*LayerInfoFetcher, error) {
	dt, err := GetFsDriver()
	if err != nil {
		return nil, err
	}
	return &LayerInfoFetcher{
		driverType:   dt,
		layerInfoMap: make(map[string]*v1.Layer),
	}, nil
}

func (ldf *LayerInfoFetcher) Fetch() ([]*v1.Layer, error) {
	l, err := driverTypes[ldf.driverType](ldf)
	return l, err
}

func (ldf *LayerInfoFetcher) fetchOverlayFs2() ([]*v1.Layer, error) {
	// scan layerdb, get diff-id to layerinfo mapping, filling size info
	td := dbDir + dir(ldf.driverType) + layerdbDir
	dirs, err := ioutil.ReadDir(td)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("-> fetcher: no layerdb dir found, %s", err))
	}

	for _, subdir := range dirs {
		chainid := subdir.Name()
		buf, _ := ioutil.ReadFile(td + dir(chainid) + diffidFile)
		diffid := string(buf)

		buf, _ = ioutil.ReadFile(td + dir(chainid) + sizeFile)
		size, _ := strconv.ParseInt(string(buf), 10, 64)
		ldf.layerInfoMap[diffid] = &v1.Layer{SizeBytes: size}
	}

	// scan diff-id-by-digest mapping, filling digest info
	td = dbDir + dir(ldf.driverType) + diffidDir
	files, err := ioutil.ReadDir(td)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("-> fetcher: no digest dir found, %s", err))
	}

	for _, f := range files {
		digest := f.Name()
		b, _ := ioutil.ReadFile(td + dir(digest))

		if l, ok := ldf.layerInfoMap[string(b)]; ok {
			l.Digest = digest
		}
	}

	l := make([]*v1.Layer, 0, len(ldf.layerInfoMap))
	for _, v := range ldf.layerInfoMap {
		// dangling diff-id may not have digest
		if v.Digest != "" && v.SizeBytes != 0 {
			v.Digest = "sha256:" + 	v.Digest
			l = append(l, v)
		}
	}
	return l, nil
}

//func (ldf *LayerInfoFetcher) fetchOverlayFs() ([]*LayerInfo, error) { return ldf.fetchOverlayFs2() }
//func (ldf *LayerInfoFetcher) fetchAufs() ([]*LayerInfo, error)      { return ldf.fetchOverlayFs2() }
func dir(str string) string { return "/" + str }
func (ldf *LayerInfoFetcher) FetchLayerSize(digests []string) []int64 {
	r := make([]int64, 0, len(digests))
	for _, d := range digests {
		r = append(r, ldf.layerInfoMap[d].SizeBytes)
	}
	return r
}