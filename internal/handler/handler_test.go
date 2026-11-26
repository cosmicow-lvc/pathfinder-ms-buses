package handler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cosmicow-lvc/pathfinder-ms-buses/internal/service"
)

type fakeDatabase struct {
	service.Repository
	err error
}

func (db fakeDatabase) Ping(context.Context) error {
	return db.err
}

func TestHealthEndpoint(t *testing.T) {
	server := httptest.NewServer(New(service.New(fakeDatabase{}, nil)))
	defer server.Close()

	response, err := http.Get(server.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, response.StatusCode)
	}
	if got := response.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("expected JSON content type, got %q", got)
	}
}

func TestReadinessEndpointReflectsDatabaseState(t *testing.T) {
	tests := []struct {
		name       string
		dbErr      error
		wantStatus int
	}{
		{name: "ready", wantStatus: http.StatusOK},
		{name: "database unavailable", dbErr: errors.New("database unavailable"), wantStatus: http.StatusServiceUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(New(service.New(fakeDatabase{err: tt.dbErr}, nil)))
			defer server.Close()

			response, err := http.Get(server.URL + "/readyz")
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()

			if response.StatusCode != tt.wantStatus {
				t.Fatalf("expected status %d, got %d", tt.wantStatus, response.StatusCode)
			}
		})
	}
}

func TestHealthEndpointRejectsNonGetMethod(t *testing.T) {
	server := httptest.NewServer(New(service.New(fakeDatabase{}, nil)))
	defer server.Close()

	request, err := http.NewRequest(http.MethodPost, server.URL+"/healthz", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, response.StatusCode)
	}
	if got := response.Header.Get("Allow"); got != http.MethodGet {
		t.Fatalf("expected Allow header %q, got %q", http.MethodGet, got)
	}
}
