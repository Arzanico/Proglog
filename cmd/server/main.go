package main

import (
	"fmt"
	"log"

	"github.com/Arzanico/proglog/internal/server"
)

func main() {
	fmt.Println("Server is up and running in port: 8080")
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
