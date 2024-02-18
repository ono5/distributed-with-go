package main

import (
	"log"

	"github.com/ono5/sample-log/internal/server"
)

func main() {
	srv := server.NewHTTPServer(":8080")
	log.Println("Start JSON Server on Port 8080....")
	log.Fatal(srv.ListenAndServe())
}
