ALTER TABLE buses
    ADD CONSTRAINT buses_estado_check
    CHECK (estado IN ('activo', 'inactivo', 'mantenimiento'));
