package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/joho/godotenv"
)

// University represents the structure of a single university entry in the database.
type University struct {
	UniversityType string          `json:"university_type"`
	Data           json.RawMessage `json:"data"`
}

// UniversityData represents the structure of the incoming JSON file.
type UniversityData struct {
	UniversitiesInNigeria2025 struct {
		FederalUniversities []map[string]interface{} `json:"federal_universities"`
		StateUniversities   []map[string]interface{} `json:"state_universities"`
		PrivateUniversities []map[string]interface{} `json:"private_universities"`
		ForeignUniversities []map[string]interface{} `json:"foreign_universities_accredited_by_nuc"`
	} `json:"universities_in_nigeria_2025"`
}

var db *pgxpool.Pool

func main() {
	// Load environment variables from .env file 
	if err := godotenv.Load(); err != nil {
		log.Printf("No .env file found, relying on system environment variables: %v", err)
	}

	// Validate DATABASE_URL
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL environment variable not set")
	}
	log.Printf("DATABASE_URL loaded: %s", databaseURL)

	var err error
	db, err = connectDB()
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	defer db.Close()

	// Confirm database connection
	log.Println("Successfully connected to the database!")

	r := chi.NewRouter()
	r.Use(middleware.RequestID) // Add request ID for tracing
	r.Use(middleware.Logger)    
	r.Use(middleware.Recoverer)
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"*"}, // Allow all origins for development
		AllowedMethods:   []string{"GET", "POST", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: false,
		MaxAge:           300,
	}))

	r.Route("/api/v1/universities", func(r chi.Router) {
		r.Post("/upload", uploadHandler)
		r.Get("/", getAllUniversitiesHandler)
		r.Get("/federal", getByTypeHandler("federal_universities"))
		r.Get("/state", getByTypeHandler("state_universities"))
		r.Get("/private", getByTypeHandler("private_universities"))
		r.Get("/foreign", getByTypeHandler("foreign_universities_accredited_by_nuc"))
	})

	log.Println("Server is running on port 8080...")
	if err := http.ListenAndServe(":8080", r); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}

// connectDB establishes a connection to the Supabase PostgreSQL database.
func connectDB() (*pgxpool.Pool, error) {
	databaseURL := os.Getenv("DATABASE_URL")
	log.Printf("Attempting to connect to database with URL: %s", databaseURL)

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("unable to parse DATABASE_URL: %v", err)
	}

	
	config.ConnConfig.PreferSimpleProtocol = true
	config.ConnConfig.DialFunc = nil

	pool, err := pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %v", err)
	}

	// Test the connection
	if err := pool.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("database ping failed: %v", err)
	}
	log.Println("Database connection established successfully")
	return pool, nil
}

// uploadHandler handles the POST request to upload the JSON data.
func uploadHandler(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.GetReqID(r.Context())
	log.Printf("Request %s: Handling POST /api/v1/universities/upload", reqID)

	if r.Method != http.MethodPost {
		log.Printf("Request %s: Invalid method: %s", reqID, r.Method)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Decode JSON
	log.Printf("Request %s: Decoding JSON request body", reqID)
	var data UniversityData
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		log.Printf("Request %s: Failed to decode JSON: %v", reqID, err)
		http.Error(w, "Invalid JSON data: "+err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("Request %s: JSON decoded, universities - federal: %d, state: %d, private: %d, foreign: %d",
		reqID,
		len(data.UniversitiesInNigeria2025.FederalUniversities),
		len(data.UniversitiesInNigeria2025.StateUniversities),
		len(data.UniversitiesInNigeria2025.PrivateUniversities),
		len(data.UniversitiesInNigeria2025.ForeignUniversities))

	tx, err := db.Begin(context.Background())
	if err != nil {
		log.Printf("Request %s: Failed to start transaction: %v", reqID, err)
		http.Error(w, "Failed to start transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(context.Background()) 

	// Insert universities
	for _, uniType := range []struct {
		name string
		data []map[string]interface{}
	}{
		{"federal_universities", data.UniversitiesInNigeria2025.FederalUniversities},
		{"state_universities", data.UniversitiesInNigeria2025.StateUniversities},
		{"private_universities", data.UniversitiesInNigeria2025.PrivateUniversities},
		{"foreign_universities_accredited_by_nuc", data.UniversitiesInNigeria2025.ForeignUniversities},
	} {
		log.Printf("Request %s: Inserting %d universities of type %s", reqID, len(uniType.data), uniType.name)
		if err := insertUniversities(tx, uniType.name, uniType.data); err != nil {
			log.Printf("Request %s: Failed to insert %s: %v", reqID, uniType.name, err)
			http.Error(w, fmt.Sprintf("Failed to insert %s: %v", uniType.name, err), http.StatusInternalServerError)
			return
		}
	}

	if err := tx.Commit(context.Background()); err != nil {
		log.Printf("Request %s: Failed to commit transaction: %v", reqID, err)
		http.Error(w, "Failed to commit transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Request %s: Data uploaded successfully", reqID)
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("Data uploaded successfully!"))
}

// insertUniversities is a helper function to insert universities of a specific type.
func insertUniversities(tx pgx.Tx, universityType string, unis []map[string]interface{}) error {
	for i, u := range unis {
		jsonData, err := json.Marshal(u)
		if err != nil {
			return fmt.Errorf("failed to marshal university data at index %d: %v", i, err)
		}

		_, err = tx.Exec(context.Background(), "INSERT INTO uni (university_type, data) VALUES ($1, $2)", universityType, jsonData)
		if err != nil {
			return fmt.Errorf("failed to insert data for university type %s at index %d: %v", universityType, i, err)
		}
	}
	return nil
}

// getAllUniversitiesHandler retrieves all universities from the database.
func getAllUniversitiesHandler(w http.ResponseWriter, r *http.Request) {
	reqID := middleware.GetReqID(r.Context())
	log.Printf("Request %s: Fetching all universities", reqID)

	universities, err := queryUniversities(context.Background(), "")
	if err != nil {
		log.Printf("Request %s: Failed to retrieve universities: %v", reqID, err)
		http.Error(w, "Failed to retrieve universities: "+err.Error(), http.StatusInternalServerError)
		return
	}
	log.Printf("Request %s: Retrieved %d universities", reqID, len(universities))
	sendJSONResponse(w, universities)
}

// getByTypeHandler returns a handler function for a specific university type.
func getByTypeHandler(uniType string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reqID := middleware.GetReqID(r.Context())
		log.Printf("Request %s: Fetching universities of type %s", reqID, uniType)

		universities, err := queryUniversities(context.Background(), uniType)
		if err != nil {
			log.Printf("Request %s: Failed to retrieve universities for type %s: %v", reqID, uniType, err)
			http.Error(w, "Failed to retrieve universities by type: "+err.Error(), http.StatusInternalServerError)
			return
		}
		log.Printf("Request %s: Retrieved %d universities for type %s", reqID, len(universities), uniType)
		sendJSONResponse(w, universities)
	}
}

// queryUniversities queries the database based on university type.
func queryUniversities(ctx context.Context, uniType string) ([]University, error) {
	var rows pgx.Rows
	var err error
	if uniType == "" {
		rows, err = db.Query(ctx, "SELECT university_type, data FROM uni")
	} else {
		rows, err = db.Query(ctx, "SELECT university_type, data FROM uni WHERE university_type = $1", uniType)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query universities: %v", err)
	}
	defer rows.Close()

	var universities []University
	for rows.Next() {
		var u University
		if err := rows.Scan(&u.UniversityType, &u.Data); err != nil {
			return nil, fmt.Errorf("failed to scan university row: %v", err)
		}
		universities = append(universities, u)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating university rows: %v", err)
	}

	return universities, nil
}

// sendJSONResponse is a helper function to send JSON responses.
func sendJSONResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("Failed to encode JSON response: %v", err)
		http.Error(w, "Failed to encode JSON response", http.StatusInternalServerError)
	}
}
