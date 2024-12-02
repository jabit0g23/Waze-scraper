CREATE KEYSPACE IF NOT EXISTS waze_traffic WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor' : 1
};

-- Crear la tabla 'jams' para almacenar datos individuales
CREATE TABLE IF NOT EXISTS waze_traffic.jams (
    idjam text PRIMARY KEY,
    commune text,
    streetName text,
    streetEnd text,
    speedKmh double,
    length double,
    timestamp timestamp
);

-- Crear la tabla 'alerts' para almacenar datos individuales
CREATE TABLE IF NOT EXISTS waze_traffic.alerts (
    idalert text PRIMARY KEY,
    commune text,
    typeAlert text,
    streetName text,
    timestamp timestamp
);

-- Crear la tabla 'jams_stats' para almacenar datos agregados
CREATE TABLE IF NOT EXISTS waze_traffic.jams_stats (
    idjam text,
    window_start timestamp,
    window_end timestamp,
    commune text,
    count bigint,
    PRIMARY KEY (idjam, window_start)
);

-- Crear la tabla 'alerts_stats' para almacenar datos agregados
CREATE TABLE IF NOT EXISTS waze_traffic.alerts_stats (
    idalert text,
    window_start timestamp,
    window_end timestamp,
    typealert text,
    count bigint,
    PRIMARY KEY (idalert, window_start)
);