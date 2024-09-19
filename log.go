package main

import (
    "net/http"
    "sync"
)

type Record struct {
    Value  []byte `json:"value"`
    Offset uint64 `json:"offset"`
}

type Log struct {
    mu      sync.Mutex
    records []Record
}

// Agregar un record al log
func (l *Log) Append(record Record) uint64 {
    l.mu.Lock()
    defer l.mu.Unlock()
    record.Offset = uint64(len(l.records)) // Asignar el offset basado en la longitud actual
    l.records = append(l.records, record)
    return record.Offset
}

// Leer un record por su offset
func (l *Log) Read(offset uint64) (Record, error) {
    l.mu.Lock()
    defer l.mu.Unlock()
    if offset >= uint64(len(l.records)) {
        return Record{}, http.ErrNotFound // Error si el offset no existe
    }
    return l.records[offset], nil
}
