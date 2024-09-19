package main

import (
        "encoding/json"
        "fmt"
        "log"
        "net/http"
        "strconv"
        "sync"

        "github.com/gorilla/mux"
)

// Record representa un registro en el commit log.
type Record struct {
        Value  []byte `json:"value"`
        Offset uint64 `json:"offset"`
}

// Log es una estructura que contiene un slice de records y un mutex para manejo concurrente.
type Log struct {
        mu       sync.Mutex
        records []Record
}

// Método para agregar un record al log
func (l *Log) Append(record Record) uint64 {
        l.mu.Lock()
        defer l.mu.Unlock()
        record.Offset = uint64(len(l.records))
        l.records = append(l.records, record)
        return record.Offset
}

// Método para leer un record por su offset
func (l *Log) Read(offset uint64) (Record, error) {
        l.mu.Lock()
        defer l.mu.Unlock()
        if offset >= uint64(len(l.records)) {
                return Record{}, fmt.Errorf("record not found")
        }
        return l.records[offset], nil
}

// Crear un nuevo Log
var logStore = &Log{}

// Handler para agregar un nuevo record (POST /log)
func handleAppend(w http.ResponseWriter, r *http.Request) {
        var record Record

        // Usar json decoder directamente en el request body
        decoder := json.NewDecoder(r.Body)
        err := decoder.Decode(&record)
        if err != nil {
                http.Error(w, "Error en el formato del JSON", http.StatusBadRequest)
                return
        }
        defer r.Body.Close() // Cerrar el cuerpo de la solicitud

        // Agregar el record al log y obtener el offset
        offset := logStore.Append(record)

        // Serializar el offset a JSON
        response := map[string]uint64{"offset": offset}
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(response)
}

// Handler para leer un record por su offset (GET /log/{offset})
func handleRead(w http.ResponseWriter, r *http.Request) {
        vars := mux.Vars(r)
        offsetStr := vars["offset"]
        offset, err := strconv.ParseUint(offsetStr, 10, 64)
        if err != nil {
                http.Error(w, "Offset inválido", http.StatusBadRequest)
                return
        }

        // Leer el record desde el log
        record, err := logStore.Read(offset)
        if err != nil {
                http.Error(w, "Record no encontrado", http.StatusNotFound)
                return
        }

        // Serializar el record a JSON
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(record)
}

func main() {
        r := mux.NewRouter()

        // Definir las rutas y sus handlers
        r.HandleFunc("/log", handleAppend).Methods("POST")
        r.HandleFunc("/log/{offset}", handleRead).Methods("GET")

        fmt.Println("Servidor corriendo en el puerto 8080...")
        log.Fatal(http.ListenAndServe(":8080", r))
}