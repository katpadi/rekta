package main

import (
  "fmt"
  "net/http"
  "log"
  "io/ioutil"
  "encoding/json"
  "github.com/go-redis/redis"
  "github.com/gorilla/mux"
)

const _rPushQueue = "go:rpush:queue"

type Payload struct {
  Id        string  `json:"id"`
  Stamps    int     `json:"stamps"`
  Username  string  `json:"username"`
}

func createNewPayload(w http.ResponseWriter, r *http.Request) {
  fmt.Println("Endpoint: createNewPayload")

  var payload Payload

  // Build Payload struct from request body
  reqBody, _ := ioutil.ReadAll(r.Body)

  // Invalid payload
  err := json.Unmarshal(reqBody, &payload)
  if err != nil {
    fmt.Println(err)
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusBadRequest)
    json.NewEncoder(w).Encode(struct{ Error string }{ "Invalid payload" })
    return
  }

  // Check that all the required data is in response body
  if payload.Id == "" {
    fmt.Println(err)
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusBadRequest)
    json.NewEncoder(w).Encode(struct{ Error string }{ "Missing ID" })
    return
  }

  // Prepare redis data
  jsonPayload, err := json.Marshal(Payload{Id: payload.Id, Stamps: payload.Stamps, Username: payload.Username})
  if err != nil {
    fmt.Println(err)
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusInternalServerError)
    json.NewEncoder(w).Encode(struct{ Error string }{ "Invalid redis payload" })
    return
  }

  // Insert to redis
  client := getRedisClient()
  err = client.RPush(_rPushQueue, jsonPayload).Err()
  if err != nil {
    fmt.Println(err)
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusInternalServerError)
    json.NewEncoder(w).Encode(struct{ Error string }{ "Write to Redis Failed" })
    return
  }

  //Write JSON of new entry if successful
  w.Header().Set("Content-Type", "application/json")
  w.WriteHeader(http.StatusCreated)
  w.Write(jsonPayload)
  return
}

func returnAllPayloads(w http.ResponseWriter, r *http.Request) {
  fmt.Println("Endpoint: returnAllPayloads")

  var payloads []Payload
  var jsonPayload []byte

  client := getRedisClient()
  redisData, err := client.LRange(_rPushQueue, 0, -1).Result()
  if err != nil {
    fmt.Println(err)
    return
  }
  fmt.Println(redisData)

  // Loop through redis data and create array
  for _, redisDatum := range redisData {
    byteRedisDatum := []byte(redisDatum)
    var payload Payload
    err = json.Unmarshal(byteRedisDatum, &payload)
    if err != nil {
      fmt.Println(err)
      return
    }
    payloads = append(payloads, payload)
  }

  w.Header().Set("Content-Type", "application/json")
  w.WriteHeader(http.StatusOK)
  jsonPayload, err = json.Marshal(payloads)
  _, err = w.Write(jsonPayload)
  return
}

func handleRequests() {
  router := mux.NewRouter().StrictSlash(true)
  router.HandleFunc("/payloads", createNewPayload).Methods("POST")
  router.HandleFunc("/payloads", returnAllPayloads)

  log.Fatal(http.ListenAndServe(":10000", router))
}

func getRedisClient() *redis.Client {
  client := redis.NewClient(&redis.Options{
    Addr: "localhost:6379",
    Password: "",
    DB: 0,
  })

  return client
}

func main() {
  handleRequests()
}
