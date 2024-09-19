package main

import (
    "encoding/json"
    "io/ioutil"
    "net/http"
    "strconv"
)

// Crear un nuevo Log
var logStore = &Log{}

// Handler para agregar un nuevo record (POST /log)
func handleAppend(w http.ResponseWriter, r *http.Request) {
    var record Record

    // Deserializar el JSON a la estructura Record
    body, err := ioutil.ReadAll(r.Body)
    if err != nil {
        http.Error(w, "Error leyendo la solicitud", http.StatusBadRequest)
        return
    }

    err = json.Unmarshal(body, &record)
    if err != nil {
        http.Error(w, "Error en el formato del JSON", http.StatusBadRequest)
        return
    }

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
