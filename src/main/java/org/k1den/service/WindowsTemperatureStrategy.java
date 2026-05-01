package org.k1den.service;

import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class WindowsTemperatureStrategy implements TemperatureStrategy {

    private static final long CACHE_TTL_MS = 10_000;
    private double cachedTemp = 0.0;
    private long lastFetchTime = 0;

    @Override
    public synchronized double getCpuTemperature() {
        long now = System.currentTimeMillis();
        if (now - lastFetchTime < CACHE_TTL_MS) {
            return cachedTemp;
        }

        try {
            String command = "powershell.exe -NoProfile -Command " +
                    "\"Get-WmiObject -Namespace root\\OpenHardwareMonitor -Class Sensor | " +
                    "Where-Object { $_.SensorType -eq 'Temperature' -and $_.Name -match 'CPU' } | " +
                    "Select-Object -First 1 -ExpandProperty Value\"";

            Process process = Runtime.getRuntime().exec(command);
            process.getOutputStream().close();

            Thread stderrDrain = new Thread(() -> {
                try { process.getErrorStream().transferTo(java.io.OutputStream.nullOutputStream()); }
                catch (Exception ignored) {}
            });
            stderrDrain.setDaemon(true);
            stderrDrain.start();

            try (Scanner scanner = new Scanner(process.getInputStream())) {
                if (scanner.hasNextDouble()) {
                    cachedTemp = scanner.nextDouble();
                    lastFetchTime = now;
                    return cachedTemp;
                }
            }

            process.waitFor(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            System.err.println("Ошибка чтения температуры: " + e.getMessage());
        }
        return 0.0;
    }
}