package main

import (
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func Status(w http.ResponseWriter, r *http.Request) {
	k := Kafka{
		servers:  "localhost",
		group:    "null",
		topic:    "Status",
		topicReg: "^aRegex.*[Ss]tatus",
	}

	err := k.Produce([]string{"Hello"})
	if err != nil {
		w.WriteHeader(http.StatusRequestTimeout)
		w.Write([]byte(err.Error()))
		return
	}
	log.Print("Prodicing is Ok\n")

	err = k.Consume()
	if err != nil {
		w.WriteHeader(http.StatusRequestTimeout)
		w.Write([]byte(err.Error()))
		return
	}
	log.Print("Prodicing is Ok\n")
}

func Publish(w http.ResponseWriter, r *http.Request) {
	k := Kafka{
		servers:  "localhost",
		group:    "myGroup",
		topic:    "OrderReceived",
		topicReg: "^aRegex.*[Oo]rder[Rr]eceived",
	}

	err := k.Produce([]string{"Hello"})
	if err != nil {
		w.WriteHeader(http.StatusRequestTimeout)
		w.Write([]byte(err.Error()))
		return
	}
}

func main() {
	r := chi.NewRouter()
	r.Use(middleware.Logger)

	r.Get("/status", Status)
	r.Get("/publish", Publish)

	log.Print("server started on port: 3000\n")
	http.ListenAndServe(":3000", r)
}
