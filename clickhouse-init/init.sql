-- 1. Таблица сырых аппаратных метрик (от агента)
CREATE TABLE IF NOT EXISTS default.device_metrics
(
    `deviceId` String,
    `deviceName` String,
    `hostname` String,
    `timestamp` UInt64,
    `cpuLoad` Float64,
    `systemLoadAverage` Float64,
    `memoryUsedPercent` Float64,
    `memoryTotal` UInt64,
    `memoryAvailable` UInt64,
    `networkRxBytes` UInt64,
    `networkTxBytes` UInt64,
    `processCount` UInt32,
    `cpuTemperature` Float64
)
    ENGINE = MergeTree
    ORDER BY (deviceId, timestamp)
    SETTINGS index_granularity = 8192;

-- 2. Таблица метрик дисков (от агента)
CREATE TABLE IF NOT EXISTS default.disk_metrics
(
    `deviceId` String,
    `timestamp` UInt64,
    `mountPoint` String,
    `total` UInt64,
    `free` UInt64,
    `usedPercent` Float64
)
    ENGINE = MergeTree
    ORDER BY (deviceId, mountPoint, timestamp)
    SETTINGS index_granularity = 8192;

-- 3. Таблица агрегированных фичей (результат работы Apache Flink)
CREATE TABLE IF NOT EXISTS default.metrics_features
(
    `deviceId` String,
    `timestamp` UInt64,
    `avgCpuLoad` Float64,
    `maxMemoryUsed` Float64,
    `avgCpuTemp` Float64,
    `avgNetRx` Float64,
    `avgNetTx` Float64,
    `avgProcesses` Float64
)
    ENGINE = MergeTree
    ORDER BY (deviceId, timestamp)
    SETTINGS index_granularity = 8192;

-- 4. Таблица настроек модели (от JavaFX интерфейса)
CREATE TABLE IF NOT EXISTS default.prediction_settings
(
    `timestamp` UInt64,
    `forecastMinutes` Int32,
    `anomalySensitivity` Float64
)
    ENGINE = MergeTree
    ORDER BY timestamp
    SETTINGS index_granularity = 8192;

-- 5. Таблица с предсказаниями (от MathEngine)
CREATE TABLE IF NOT EXISTS default.predictions
(
    `deviceId` String,
    `metricName` String,
    `createdAt` UInt64,
    `forecastTime` UInt64,
    `predictedValue` Float64,
    `status` String,
    `reason` String
)
    ENGINE = MergeTree
    ORDER BY (deviceId, metricName, createdAt)
    SETTINGS index_granularity = 8192;

INSERT INTO default.prediction_settings (timestamp, forecastMinutes, anomalySensitivity)
SELECT toUnixTimestamp(now()) * 1000, 15, 3.0
    WHERE NOT EXISTS (SELECT 1 FROM default.prediction_settings);