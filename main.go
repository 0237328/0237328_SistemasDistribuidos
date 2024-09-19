package main

import (
    "fmt"
    "log"
    "net/http"

    "github.com/gorilla/mux"
)

func main() {
    r := mux.NewRouter()

    // Rutas y sus handlers
    r.HandleFunc("/log", handleAppend).Methods("POST")
    r.HandleFunc("/log/{offset}", handleRead).Methods("GET")

    fmt.Println("Servidor corriendo en el puerto 8080...")
    log.Fatal(http.ListenAndServe(":8080", r))
}