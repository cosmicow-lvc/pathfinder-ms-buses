package domain

import "time"

const (
	BusStatusActive      = "activo"
	BusStatusInactive    = "inactivo"
	BusStatusMaintenance = "mantenimiento"

	BusValidationReasonNotFound = "BUS_NOT_FOUND"
	BusValidationReasonInactive = "BUS_NOT_ACTIVE"
)

type Bus struct {
	ID        string    `json:"id"`
	DriverID  string    `json:"driverId"`
	Patente   string    `json:"patente"`
	Empresa   string    `json:"empresa"`
	Estado    string    `json:"estado"`
	UpdatedAt time.Time `json:"updatedAt"`
	CreatedAt time.Time `json:"createdAt"`
}

type BusInput struct {
	DriverID string `json:"driverId"`
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

type BusValidation struct {
	BusID  string  `json:"busId"`
	Valid  bool    `json:"valid"`
	Estado *string `json:"estado"`
	Reason *string `json:"reason"`
}
