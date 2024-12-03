-- Crear el keyspace 'waze_traffic' si no existe
CREATE KEYSPACE IF NOT EXISTS waze_traffic WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor' : 1
};

-- Tabla 'jams' para almacenar datos históricos de atascos
CREATE TABLE IF NOT EXISTS waze_traffic.jams (
    idjam text PRIMARY KEY,
    commune text,
    country text,
    streetname text,
    streetend text,
    speedkmh double,
    length double,
    timestamp timestamp
);

-- Tabla 'alerts' para almacenar datos históricos de alertas
CREATE TABLE IF NOT EXISTS waze_traffic.alerts (
    idalert text PRIMARY KEY,
    commune text,
    country text,
    typealert text,
    streetname text,
    timestamp timestamp
);
