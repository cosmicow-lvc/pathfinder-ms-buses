package domain

import "errors"

var (
	ErrNotFound             = errors.New("resource not found")
	ErrInvalidID            = errors.New("invalid UUID")
	ErrInvalidInput         = errors.New("invalid input")
	ErrPatenteAlreadyExists = errors.New("patente already exists")
	ErrRUTAlreadyExists     = errors.New("RUT already exists")
	ErrDriverNotFound       = errors.New("driver not found")
)
