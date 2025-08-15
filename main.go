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
		log.Fatalf("Error loading .env file: %v", err)
	}

	var err error
	db, err = connectDB()
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	defer db.Close()

	// Print statement to confirm database connection
	fmt.Println("Successfully connected to the database!")

	r := chi.NewRouter()
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

	fmt.Println("Server is running on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", r))
}

// connectDB establishes a connection to the Supabase PostgreSQL database.
func connectDB() (*pgxpool.Pool, error) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL environment variable not set")
	}

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("unable to parse DATABASE_URL: %v", err)
	}

	pool, err := pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %v", err)
	}

	return pool, nil
}

// uploadHandler handles the POST request to upload the JSON data.
func uploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Read the entire request body
	var data UniversityData
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, "Invalid JSON data: "+err.Error(), http.StatusBadRequest)
		return
	}

	tx, err := db.Begin(context.Background())
	if err != nil {
		http.Error(w, "Failed to start transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(context.Background()) // Rollback on error

	// Insert federal universities
	err = insertUniversities(tx, "federal_universities", data.UniversitiesInNigeria2025.FederalUniversities)
	if err != nil {
		http.Error(w, "Failed to insert federal universities: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Insert state universities
	err = insertUniversities(tx, "state_universities", data.UniversitiesInNigeria2025.StateUniversities)
	if err != nil {
		http.Error(w, "Failed to insert state universities: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Insert private universities
	err = insertUniversities(tx, "private_universities", data.UniversitiesInNigeria2025.PrivateUniversities)
	if err != nil {
		http.Error(w, "Failed to insert private universities: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Insert foreign accredited universities
	err = insertUniversities(tx, "foreign_universities_accredited_by_nuc", data.UniversitiesInNigeria2025.ForeignUniversities)
	if err != nil {
		http.Error(w, "Failed to insert foreign universities: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(context.Background()); err != nil {
		http.Error(w, "Failed to commit transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("Data uploaded successfully!"))
}

// insertUniversities is a helper function to insert universities of a specific type.
func insertUniversities(tx pgx.Tx, universityType string, unis []map[string]interface{}) error {
	for _, u := range unis {
		jsonData, err := json.Marshal(u)
		if err != nil {
			return fmt.Errorf("failed to marshal university data: %v", err)
		}

		_, err = tx.Exec(context.Background(), "INSERT INTO uni (university_type, data) VALUES ($1, $2)", universityType, jsonData)
		if err != nil {
			return fmt.Errorf("failed to insert data for university type %s: %v", universityType, err)
		}
	}
	return nil
}

// getAllUniversitiesHandler retrieves all universities from the database.
func getAllUniversitiesHandler(w http.ResponseWriter, r *http.Request) {
	universities, err := queryUniversities(context.Background(), "")
	if err != nil {
		http.Error(w, "Failed to retrieve universities: "+err.Error(), http.StatusInternalServerError)
		return
	}
	sendJSONResponse(w, universities)
}

// getByTypeHandler returns a handler function for a specific university type.
func getByTypeHandler(uniType string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		universities, err := queryUniversities(context.Background(), uniType)
		if err != nil {
			http.Error(w, "Failed to retrieve universities by type: "+err.Error(), http.StatusInternalServerError)
			return
		}
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
		return nil, err
	}
	defer rows.Close()

	var universities []University
	for rows.Next() {
		var u University
		if err := rows.Scan(&u.UniversityType, &u.Data); err != nil {
			return nil, err
		}
		universities = append(universities, u)
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return universities, nil
}

// sendJSONResponse is a helper function to send JSON responses.
func sendJSONResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		http.Error(w, "Failed to encode JSON response", http.StatusInternalServerError)
	}
}
