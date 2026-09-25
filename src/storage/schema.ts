/**
 * src/storage/schema.ts
 *
 * DuckDB table definitions and index declarations for the telemetry
 * persistence layer. Kept in one place so the batch writer and the
 * query interface always agree on column names and types.
 */

export const TELEMETRY_TABLE = "telemetry";
export const ANOMALIES_TABLE = "anomalies";

export const CREATE_TELEMETRY_TABLE = `
  CREATE TABLE IF NOT EXISTS ${TELEMETRY_TABLE} (
    sequence_id   UINTEGER  NOT NULL,
    timestamp     DOUBLE    NOT NULL,
    car_id        UTINYINT  NOT NULL,
    gear          TINYINT   NOT NULL,
    speed         DOUBLE    NOT NULL,
    rpm           DOUBLE    NOT NULL,
    throttle      DOUBLE    NOT NULL,
    brake         DOUBLE    NOT NULL,
    tire_temp_fl  DOUBLE    NOT NULL,
    tire_temp_fr  DOUBLE    NOT NULL,
    tire_temp_rl  DOUBLE    NOT NULL,
    tire_temp_rr  DOUBLE    NOT NULL
  )
`;

export const CREATE_ANOMALIES_TABLE = `
  CREATE TABLE IF NOT EXISTS ${ANOMALIES_TABLE} (
    sequence_id  UINTEGER  NOT NULL,
    timestamp    DOUBLE    NOT NULL,
    car_id       UTINYINT  NOT NULL,
    kind         VARCHAR   NOT NULL,
    severity     VARCHAR   NOT NULL,
    field        VARCHAR   NOT NULL,
    value        DOUBLE    NOT NULL,
    threshold    DOUBLE    NOT NULL,
    message      VARCHAR   NOT NULL
  )
`;

export const CREATE_TELEMETRY_INDEXES = [
  `CREATE INDEX IF NOT EXISTS idx_telemetry_car_id
     ON ${TELEMETRY_TABLE} (car_id)`,
  `CREATE INDEX IF NOT EXISTS idx_telemetry_timestamp
     ON ${TELEMETRY_TABLE} (timestamp)`,
];

export const CREATE_ANOMALY_INDEXES = [
  `CREATE INDEX IF NOT EXISTS idx_anomalies_car_id
     ON ${ANOMALIES_TABLE} (car_id)`,
  `CREATE INDEX IF NOT EXISTS idx_anomalies_kind
     ON ${ANOMALIES_TABLE} (kind)`,
];