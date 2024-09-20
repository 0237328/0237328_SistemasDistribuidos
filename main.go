package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	"github.com/0237328/0237328_SistemasDistribuidos/log"
	"github.com/gorilla/mux"
)

type Config struct {
	Segment struct {
		MaxStoreBytes uint64
		MaxIndexBytes uint64
	}
}

type Log struct {
	mu            sync.RWMutex
	Dir           string
	Config        Config
	activeSegment *Log.segment
	segments      []*Log.segment
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log{
		Dir:    dir,
		Config: c,
	}
	return l, l.setup()
}

func (l *Log) setup() error {
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}
	var baseOffsets []uint64
	for _, file := range files {
		offStr := file.Name()
		off, _ := strconv.ParseUint(filepath.Base(offStr), 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})
	for _, baseOffset := range baseOffsets {
		if err := l.newSegment(baseOffset); err != nil {
			return err
		}
	}
	if l.segments == nil {
		if err := l.newSegment(l.Config.Segment.MaxStoreBytes); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) newSegment(baseOffset uint64) error {
	s, err := log.NewSegment(l.Dir, baseOffset, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}

func (l *Log) Append(record []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}
	return off, err
}

func (l *Log) Read(offset uint64) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	for _, segment := range l.segments {
		if segment.baseOffset <= offset && offset < segment.nextOffset {
			return segment.Read(offset)
		}
	}
	return nil, fmt.Errorf("offset out of range")
}

func main() {
	dir := "./logdata"
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatalf("could not create log directory: %v", err)
	}

	config := Config{}
	config.Segment.MaxStoreBytes = 1024
	config.Segment.MaxIndexBytes = 1024

	logStore, err := NewLog(dir, config)
	if err != nil {
		log.Fatalf("could not initialize log: %v", err)
	}

	// Crear enrutador con Gorilla Mux
	r := mux.NewRouter()

	// Endpoint para escribir un registro
	r.HandleFunc("/log", func(w http.ResponseWriter, r *http.Request) {
		var record []byte
		_, err := r.Body.Read(record)
		if err != nil {
			http.Error(w, "error reading request body", http.StatusBadRequest)
			return
		}

		offset, err := logStore.Append(record)
		if err != nil {
			http.Error(w, "could not append log", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Record stored at offset: %d\n", offset)
	}).Methods("POST")

	// Endpoint para leer un registro por su offset
	r.HandleFunc("/log/{offset}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		offset, err := strconv.ParseUint(vars["offset"], 10, 64)
		if err != nil {
			http.Error(w, "invalid offset", http.StatusBadRequest)
			return
		}

		record, err := logStore.Read(offset)
		if err != nil {
			http.Error(w, "could not read record", http.StatusNotFound)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write(record)
	}).Methods("GET")

	fmt.Println("Servidor corriendo en el puerto 8080...")
	log.Fatal(http.ListenAndServe(":8080", r))
}
