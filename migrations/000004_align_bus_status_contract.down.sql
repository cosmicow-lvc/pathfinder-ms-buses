UPDATE buses
SET estado = CASE estado
    WHEN 'ACTIVO' THEN 'activo'
    WHEN 'INACTIVO' THEN 'inactivo'
    WHEN 'EN_MANTENCION' THEN 'mantenimiento'
    ELSE estado
END;

ALTER TABLE buses DROP CONSTRAINT IF EXISTS buses_estado_check;
ALTER TABLE buses
    ADD CONSTRAINT buses_estado_check
    CHECK (estado IN ('activo', 'inactivo', 'mantenimiento'));
