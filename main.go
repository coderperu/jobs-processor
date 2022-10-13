package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/mux"
)

const (
	AUTH_HEADER      = "Authorization"
	AUTH_VALUE       = "allow"
	UNAUTHORIZED     = "Unauthorized to access this resource"
	STATUS_PENDING   = "pending"
	STATUS_PROCESSED = "processed"
	DEFAULT_INTERVAL = 10 //seconds
	ENV_INTERVAL     = "ENV_INTERVAL"
)

type Job struct {
	Name   string `json:"name"`
	Id     int    `json:"id"`
	Status string `json:"status"`
	Data   []int  `json:"data"`
	Result string `json:"result"`
}

type ErrorResponse struct {
	Error  string `json:"error"`
	Status int    `json:"status"`
}

type JobsQueue []*Job

var jobsQueue JobsQueue

func (jq *JobsQueue) Add(job *Job) {
	job.Id = len(jobsQueue) + 1
	job.Status = STATUS_PENDING
	*jq = append(*jq, job)
}

func (jq *JobsQueue) GetJobs() []*Job {
	return *jq
}

func (jq *JobsQueue) GetJobsByStatus(status string) []*Job {
	var result []*Job

	for _, job := range jobsQueue {
		if job.Status == status {
			result = append(result, job)
		}
	}
	return result
}

func getJobs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobsQueue.GetJobs())
}

func createJob(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var job Job
	_ = json.NewDecoder(r.Body).Decode(&job)
	jobsQueue.Add(&job)
	json.NewEncoder(w).Encode(&job)
}

func getJobsByStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)
	json.NewEncoder(w).Encode(jobsQueue.GetJobsByStatus(params["status"]))
}

func authMiddleware(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(AUTH_HEADER) == AUTH_VALUE {
			handler.ServeHTTP(w, r)
		} else {
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(ErrorResponse{Error: UNAUTHORIZED, Status: http.StatusUnauthorized})
		}
	})
}

func backgroundTask() {
	jobs := make(chan *Job, 1)
	var wg sync.WaitGroup

	wg.Add(1)

	go worker(jobs, &wg)

	for _, job := range jobsQueue {
		if job.Status == STATUS_PENDING {
			jobs <- job
		}
	}

	close(jobs)
	wg.Wait()
}

func worker(jobs chan *Job, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobs {
		jobProcessor(job)
	}
}

func jobProcessor(job *Job) {
	result := 0
	for _, val := range job.Data {
		result += val
	}

	job.Result = fmt.Sprintf("Result is %d", result)
	job.Status = STATUS_PROCESSED
}

func ticker_clock(ticker *time.Ticker) {
	for t := range ticker.C {
		fmt.Println("Execute job processing at ", t)

		go backgroundTask()
	}
}

func main() {
	var jobInterval int
	jobInterval, err := strconv.Atoi(os.Getenv(ENV_INTERVAL))

	if err != nil {
		jobInterval = DEFAULT_INTERVAL
	}

	ticker := time.NewTicker(time.Second * time.Duration(jobInterval))

	go ticker_clock(ticker)

	router := mux.NewRouter()
	handler := authMiddleware(router)

	router.HandleFunc("/jobs", getJobs).Methods("GET")
	router.HandleFunc("/jobs", createJob).Methods("POST")
	router.HandleFunc("/jobs/{status}", getJobsByStatus).Methods("GET")

	http.ListenAndServe(":8000", handler)
}
