CREATE KEYSPACE IF NOT EXISTS waze_traffic WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor' : 1
};

-- Crear la tabla 'jams' para almacenar datos históricos de atascos
CREATE TABLE IF NOT EXISTS waze_traffic.jams (
    idjam text PRIMARY KEY,           -- Identificador único del atasco
    commune text,                     -- Comuna
    streetname text,                  -- Nombre de la calle
    streetend text,                   -- Final del atasco
    speedkmh double,                  -- Velocidad promedio en el atasco
    length double,                    -- Longitud del atasco
    timestamp timestamp               -- Momento en que ocurrió el atasco
);

-- Crear la tabla 'alerts' para almacenar datos históricos de alertas
CREATE TABLE IF NOT EXISTS waze_traffic.alerts (
    idalert text PRIMARY KEY,         -- Identificador único de la alerta
    commune text,                     -- Comuna
    typealert text,                   -- Tipo de alerta (accidente, policía, etc.)
    streetname text,                  -- Calle donde ocurrió la alerta
    timestamp timestamp               -- Momento en que ocurrió la alerta
);

-- Crear la tabla 'static_data' para almacenar datos estáticos
CREATE TABLE IF NOT EXISTS waze_traffic.static_data (
    id text PRIMARY KEY,              -- Identificador único del dato estático
    bbox text,                        -- Coordenadas geográficas (ej., límites de la comuna)
    streetname text,                  -- Nombre de la calle (si aplica)
    commune text,                     -- Comuna o ciudad
    description text                  -- Descripción general del dato
);
