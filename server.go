package main

import (
    "encoding/json"
    "fmt"
    "log"
    "net/http"
)

// Definir una estructura de Usuario
type Usuario struct {
    Nombre string `json:"nombre"`
    Email  string `json:"email"`
}

var usuarios = []Usuario{} // Slice para almacenar usuarios

// Handler para agregar un usuario (POST /agregar)
func agregarUsuario(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "Método no permitido", http.StatusMethodNotAllowed)
        return
    }

    var nuevoUsuario Usuario
    err := json.NewDecoder(r.Body).Decode(&nuevoUsuario)
    if err != nil {
        http.Error(w, "Error en el formato del JSON", http.StatusBadRequest)
        return
    }

    usuarios = append(usuarios, nuevoUsuario)

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]string{"message": "Usuario agregado exitosamente"})
}

// Handler para obtener la lista de usuarios (GET /usuarios)
func obtenerUsuarios(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(usuarios)
}

func main() {
    // Rutas y handlers
    http.HandleFunc("/agregar", agregarUsuario)
    http.HandleFunc("/usuarios", obtenerUsuarios)

    fmt.Println("Servidor corriendo en el puerto 8080...")
    log.Fatal(http.ListenAndServe(":8080", nil))
}
