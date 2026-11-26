package domain

import "time"

type Bus struct {
	ID        string    `json:"id"`
	DriverID  string    `json:"driver_id"`
	Patente   string    `json:"patente"`
	Empresa   string    `json:"empresa"`
	Estado    string    `json:"estado"`
	UpdatedAt time.Time `json:"updated_at"`
	CreatedAt time.Time `json:"created_at"`
}

type BusInput struct {
	DriverID string `json:"driver_id"`
	Patente  string `json:"patente"`
	Empresa  string `json:"empresa"`
	Estado   string `json:"estado"`
}

type Driver struct {
	ID       string `json:"id"`
	Nombre   string `json:"nombre"`
	Telefono string `json:"telefono"`
	RUT      string `json:"rut"`
	Activo   bool   `json:"activo"`
}

type DriverInput struct {
	Nombre   string `json:"nombre"`
	Telefono string `json:"telefono"`
	RUT      string `json:"rut"`
}

type DriverUpdateInput struct {
	Nombre   string `json:"nombre"`
	Telefono string `json:"telefono"`
	RUT      string `json:"rut"`
	Activo   bool   `json:"activo"`
}
