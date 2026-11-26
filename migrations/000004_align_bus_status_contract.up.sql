UPDATE buses
SET estado = CASE estado
    WHEN 'activo' THEN 'ACTIVO'
    WHEN 'inactivo' THEN 'INACTIVO'
    WHEN 'mantenimiento' THEN 'EN_MANTENCION'
    ELSE estado
END;

ALTER TABLE buses DROP CONSTRAINT IF EXISTS buses_estado_check;
ALTER TABLE buses
    ADD CONSTRAINT buses_estado_check
    CHECK (estado IN ('ACTIVO', 'INACTIVO', 'EN_MANTENCION'));
