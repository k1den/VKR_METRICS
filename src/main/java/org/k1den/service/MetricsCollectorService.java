package org.k1den.service;

import org.k1den.config.AppConfig;
import org.k1den.kafka.KafkaProducerService;
import org.k1den.model.DiskUsage;
import org.k1den.model.Metric;
import org.k1den.util.JsonUtils;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.software.os.FileSystem;
import oshi.software.os.OSFileStore;
import oshi.software.os.OperatingSystem;
import oshi.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class MetricsCollectorService {
    private final AppConfig cfg;
    private final KafkaProducerService producer;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final SystemInfo si = new SystemInfo();
    private final HardwareAbstractionLayer hal = si.getHardware();
    private final OperatingSystem os = si.getOperatingSystem();
    private volatile int intervalSeconds;
    private ScheduledFuture<?> scheduledTask;

    private volatile long lastRx = 0;
    private volatile long lastTx = 0;
    private volatile long lastNetworkTime = 0;

    private long[] prevCpuTicks = null;

    private final TemperatureStrategy temperatureStrategy;

    public MetricsCollectorService(AppConfig cfg, KafkaProducerService producer) {
        this.cfg = cfg;
        this.producer = producer;
        this.intervalSeconds = Math.max(5, Math.min(60, cfg.initialIntervalSeconds));

        String osName = System.getProperty("os.name").toLowerCase();
        if (osName.contains("win")) {
            this.temperatureStrategy = new WindowsTemperatureStrategy();
            System.out.println("Initialized Windows Temperature Strategy (OHM)");
        } else {
            this.temperatureStrategy = new LinuxTemperatureStrategy();
            System.out.println("Initialized Linux Temperature Strategy (sysfs)");
        }
    }

    public synchronized void start() {
        if (scheduledTask != null && !scheduledTask.isCancelled()) return;
        scheduledTask = scheduler.scheduleAtFixedRate(this::collectAndSend, 0, intervalSeconds, TimeUnit.SECONDS);
        System.out.println("MetricsCollector started with interval " + intervalSeconds + "s");
    }

    private void collectAndSend() {
        try {
            Metric m = collect();
            String json = JsonUtils.toJson(m);
            producer.send(m.hostname + "-" + m.timestamp, json);
            System.out.println("Sent metric: " + m.hostname + " ts=" + m.timestamp);
        } catch (Exception e) {
            System.err.println("Failed to collect/send metrics: " + e.getMessage());
        }
    }

    private Metric collect() {
        Metric m = new Metric();
        m.deviceId = cfg.deviceId;
        m.deviceName = cfg.deviceName;
        m.hostname = cfg.hostname;
        m.timestamp = System.currentTimeMillis();

        // ----- CPU -----
        CentralProcessor cpu = hal.getProcessor();
        long[] ticks = cpu.getSystemCpuLoadTicks();
        if (prevCpuTicks == null) {
            prevCpuTicks = ticks;
            Util.sleep(100);
            ticks = cpu.getSystemCpuLoadTicks();
        }
        double cpuLoad = cpu.getSystemCpuLoadBetweenTicks(prevCpuTicks) * 100.0;
        prevCpuTicks = ticks;
        m.cpuLoad = (!Double.isNaN(cpuLoad) && cpuLoad >= 0) ? Math.round(cpuLoad * 100.0) / 100.0 : 0.0;

        // ----- CPU Temperature (РЕАЛЬНАЯ ЧЕРЕЗ OHM) -----
        double temp = temperatureStrategy.getCpuTemperature();

        if (temp <= 0.0) {
            m.cpuTemperature = Double.NaN;
        } else {
            m.cpuTemperature = Math.round(temp * 10.0) / 10.0;
        }

        // ----- Memory -----
        GlobalMemory mem = hal.getMemory();
        m.memoryTotal = mem.getTotal();
        m.memoryAvailable = mem.getAvailable();
        double usedPct = (double) (m.memoryTotal - m.memoryAvailable) / m.memoryTotal * 100.0;
        m.memoryUsedPercent = Math.round(usedPct * 100.0) / 100.0;

        // ----- Disks -----
        FileSystem fs = os.getFileSystem();
        List<DiskUsage> disks = new ArrayList<>();
        for (OSFileStore s : fs.getFileStores()) {
            long total = s.getTotalSpace();
            long free = s.getUsableSpace();
            double usedPercent = total > 0 ? (double) (total - free) / total * 100.0 : 0.0;
            disks.add(new DiskUsage(s.getMount(), total, free, Math.round(usedPercent * 100.0) / 100.0));
        }
        m.disks = disks;

        // ----- Network (Байт/сек) -----
        long currentRx = 0, currentTx = 0;
        try {
            for (var n : hal.getNetworkIFs()) {
                currentRx += n.getBytesRecv();
                currentTx += n.getBytesSent();
            }
        } catch (Throwable ignored) {}

        long currentTime = System.currentTimeMillis();
        if (lastNetworkTime == 0) {
            lastRx = currentRx;
            lastTx = currentTx;
            lastNetworkTime = currentTime;
            m.networkRxBytes = 0;
            m.networkTxBytes = 0;
        } else {
            long timeDelta = currentTime - lastNetworkTime;
            if (timeDelta > 0) {
                m.networkRxBytes = Math.max(0, (currentRx - lastRx) * 1000 / timeDelta);
                m.networkTxBytes = Math.max(0, (currentTx - lastTx) * 1000 / timeDelta);
            }
            lastRx = currentRx;
            lastTx = currentTx;
            lastNetworkTime = currentTime;
        }

        // ----- Process Count -----
        m.processCount = os.getProcessCount();
        m.tags = cfg.tags;

        return m;
    }

    public synchronized void updateInterval(int newIntervalSeconds) {
        if (newIntervalSeconds < 5) newIntervalSeconds = 5;
        if (newIntervalSeconds > 60) newIntervalSeconds = 60;
        if (newIntervalSeconds == this.intervalSeconds) return;
        this.intervalSeconds = newIntervalSeconds;
        if (scheduledTask != null) scheduledTask.cancel(false);
        scheduledTask = scheduler.scheduleAtFixedRate(this::collectAndSend, 0, intervalSeconds, TimeUnit.SECONDS);
        System.out.println("Collector interval updated to " + intervalSeconds + " seconds");
    }

    public void shutdown() {
        try { scheduler.shutdownNow(); } catch (Exception ignored) {}
    }
}