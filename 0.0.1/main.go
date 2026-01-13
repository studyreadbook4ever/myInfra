/* Controlling MicroService by Go language 
* this program need to be connected by data plane- with json. 
* usage: go run main.go -port <portnumber> then it will opened on 127.0.0.1:8080
* we give 'graceful sigterm' and we scheduling with node's score(danger to starvation but it is better than RR for low performance, low MicroServices
*/


package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"
)


type NodeStats struct {
	CpuUsage    float64 `json:"cpu_usage"`    /*  0.0 ~ 100.0% */
	MemFreeMB   int64   `json:"mem_free_mb"`  /* Available Memory in MB */
	ActiveCount int     `json:"active_count"` /* Number of running processes */
}

type Node struct {
	ID       string    `json:"id"`
	IP       string    `json:"ip"`
	Stats    NodeStats `json:"stats"` /* func handleHeartbeat  */
	LastSeen time.Time `json:"last_seen"`
	Status   string    `json:"status"` /*  Healthy- Unknown- Dead */
}

type Task struct {
	ID            string    `json:"id"`
	Image         string    `json:"image"`
	TargetStatus  string    `json:"target_status"`  /* User intent: Running, Stopped */
	CurrentStatus string    `json:"current_status"` /* real state: Pending, Running, Stopped */
	AssignedNode  string    `json:"assigned_node,omitempty"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type ClusterState struct {
	Tasks map[string]*Task `json:"tasks"`
	Nodes map[string]*Node `json:"nodes"`
	

	mu sync.RWMutex `json:"-"`
}

var (
	dbFile          = "db.json" /* modify db filename */
	authToken       = "AUTH-TOKEN" /* real world- we need TLS Authority. */
	state           = &ClusterState{Tasks: make(map[string]*Task), Nodes: make(map[string]*Node)}
	
	NodeTimeout     = 30 * time.Second 
	EvictionTimeout = 300 * time.Second  
)
/*taking Consistency, Partition Tolerance> Sync hardly */
func saveInternal() error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil { return err }

	tmpFile := dbFile + ".tmp"
	f, err := os.OpenFile(tmpFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|syscall.O_SYNC, 0644)
	if err != nil { return err }

	if _, err := f.Write(data); err != nil { f.Close(); return err }
	if err := f.Sync(); err != nil { f.Close(); return err } // Double tap to be sure
	f.Close()

	return os.Rename(tmpFile, dbFile)
}

func persistState() error {
	state.mu.Lock()
	defer state.mu.Unlock()
	return saveInternal()
}
/*getting saved state */
func loadState() {
	state.mu.Lock()
	defer state.mu.Unlock()

	data, err := os.ReadFile(dbFile)
	if err == nil {
		json.Unmarshal(data, state)
		log.Printf("[Init] Cluster State Loaded: %d Tasks, %d Nodes", len(state.Tasks), len(state.Nodes))
	} else {
		log.Println("[Init] Starting fresh cluster.")
	}
}


/* evaluate node's score */
func calculateScore(n *Node) int {
	score := int(100.0 - n.Stats.CpuUsage)

	score -= (n.Stats.ActiveCount * 5)

	if n.Stats.MemFreeMB > 1024 {
		score += 10
	} else if n.Stats.MemFreeMB < 128 {
		score -= 20 
	}

	if score < 0 { return 0 }
	return score
}

func findBestNode(nodes map[string]*Node) string {
	bestID := ""
	maxScore := -1

	for id, node := range nodes {
		if node.Status != "Healthy" { continue }

		score := calculateScore(node)
		if score > maxScore {
			maxScore = score
			bestID = id
		}
	}

	if maxScore <= 0 { return "" }
	return bestID
}

func runScheduler(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			state.mu.Lock() 
			
			dirty := false
			now := time.Now()
			activeNodeCount := 0

			/* --- Phase 1: Health Check & Eviction --- */
			for id, node := range state.Nodes {
				since := now.Sub(node.LastSeen)

				if since > EvictionTimeout {
					if node.Status != "Dead" {
						log.Printf("[Eviction] Node %s is DEAD. Rescuing tasks...", id)
						node.Status = "Dead"
						dirty = true

						for _, t := range state.Tasks {
							if t.AssignedNode == id {
								t.AssignedNode = "" 
								t.CurrentStatus = "Pending"
								t.UpdatedAt = now
							}
						}
					}
				} else if since > NodeTimeout {
					if node.Status != "Unknown" {
						node.Status = "Unknown"
						dirty = true
					}
				} else {
					node.Status = "Healthy"
					activeNodeCount++
				}
			}

			/* --- Phase 2: Smart Scheduling --- */
			if activeNodeCount > 0 {
				pendingTasks := []*Task{}
				for _, t := range state.Tasks {
					if t.TargetStatus == "Running" && t.AssignedNode == "" {
						pendingTasks = append(pendingTasks, t)
					}
				}

				for _, t := range pendingTasks {
					target := findBestNode(state.Nodes)
					
					if target != "" {
						t.AssignedNode = target
						t.CurrentStatus = "Scheduled"
						t.UpdatedAt = now
						log.Printf("[Scheduler] Assigned %s -> %s (Score Optimized)", t.ID, target)
						dirty = true

						if n, ok := state.Nodes[target]; ok {
							n.Stats.ActiveCount++
						}
					}
				}
			}

			/* --- Phase 3: Cleanup --- */
			for id, t := range state.Tasks {
				if t.TargetStatus == "Stopped" && t.CurrentStatus == "Stopped" {
					log.Printf("[Cleanup] Removed completed task %s", id)
					delete(state.Tasks, id)
					dirty = true
				}
			}

			/* --- Phase 4: Persistence --- */
			if dirty {
				if err := saveInternal(); err != nil {
					log.Printf("[Error] Failed to persist state: %v", err)
				}
			}

			state.mu.Unlock()
		}
	}
}


func authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("Authorization")
		expected := "Bearer " + authToken
		if subtle.ConstantTimeCompare([]byte(token), []byte(expected)) != 1 {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

// [POST] /deploy - Accept new workloads
func handleDeploy(w http.ResponseWriter, r *http.Request) {
	var req Task
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", 400); return
	}

	state.mu.Lock()
	req.TargetStatus = "Running"
	req.CurrentStatus = "Pending"
	req.UpdatedAt = time.Now()
	state.Tasks[req.ID] = &req
	
	err := saveInternal() 
	state.mu.Unlock()

	if err != nil { http.Error(w, "Persistence Error", 500); return }
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, "Task %s deployed.\n", req.ID)
}

// [POST] /stop?id=... - Graceful Shutdown Signal
func handleStop(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	state.mu.Lock()
	if t, exists := state.Tasks[id]; exists {
		t.TargetStatus = "Stopped"
		t.UpdatedAt = time.Now()
		saveInternal()
	}
	state.mu.Unlock()
	w.Write([]byte("Stop signal sent for " + id))
}

// [POST] /heartbeat - Worker Check-in
func handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var n Node
	if err := json.NewDecoder(r.Body).Decode(&n); err != nil {
		http.Error(w, "Invalid JSON", 400); return
	}

	state.mu.Lock()
	existingNode, exists := state.Nodes[n.ID]
	
	if !exists || existingNode.Status == "Dead" {
		n.Status = "Healthy"
		n.LastSeen = time.Now()
		state.Nodes[n.ID] = &n 
	} else {
		existingNode.LastSeen = time.Now()
		existingNode.Status = "Healthy"
		existingNode.IP = n.IP
		existingNode.Stats = n.Stats /* <--- Update stats coming from worker */
	}
	state.mu.Unlock() // Release Write Lock Immediately!

	/* RLock allows multiple cores to fetch tasks concurrently */
	state.mu.RLock()
	var myTasks []*Task
	for _, t := range state.Tasks {
		if t.AssignedNode == n.ID {
			myTasks = append(myTasks, t)
		}
	}
	state.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"tasks": myTasks,
	})
}

// [POST] /status - Worker Status Report
func handleStatusUpdate(w http.ResponseWriter, r *http.Request) {
	var report struct {
		TaskID string `json:"task_id"`
		Status string `json:"status"` /* Running-Stopped-Failed */
	}
	json.NewDecoder(r.Body).Decode(&report)
	
	state.mu.Lock()
	if t, exists := state.Tasks[report.TaskID]; exists {
		t.CurrentStatus = report.Status
		t.UpdatedAt = time.Now()
		/* No disk sync here to avoid I/O spam.*/
	}
	state.mu.Unlock()
}

// [GET] /state - Dashboard Data
func handleState(w http.ResponseWriter, r *http.Request) {
	state.mu.RLock() 
	defer state.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(state)
}


func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	port := flag.String("port", "8080", "Server Port") /* check portnum!!! default:8080 */
	flag.Parse()

	log.Printf("Initializing Web-Maestro (CPUs: %d)...", runtime.NumCPU())
	
	loadState()
	ctx, cancel := context.WithCancel(context.Background())
	go runScheduler(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/deploy", authMiddleware(handleDeploy))
	mux.HandleFunc("/stop", authMiddleware(handleStop))
	mux.HandleFunc("/heartbeat", authMiddleware(handleHeartbeat))
	mux.HandleFunc("/status", authMiddleware(handleStatusUpdate))
	mux.HandleFunc("/state", authMiddleware(handleState))

	srv := &http.Server{
		Addr:         ":" + *port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("Orchestrator listening on :%s", *port)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down gracefully...")
	cancel() /* Stop Scheduler */
	srv.Shutdown(context.Background())

	/*final saving */
	log.Println("Saving final state to disk...")
	persistState()
	log.Println("Goodbye.")
}
