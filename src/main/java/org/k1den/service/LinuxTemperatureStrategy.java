package org.k1den.service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class LinuxTemperatureStrategy implements TemperatureStrategy {
    private final Path tempFilePath;

    public LinuxTemperatureStrategy() {
        this(Paths.get("/sys/class/thermal/thermal_zone0/temp"));
    }

    public LinuxTemperatureStrategy(Path tempFilePath) {
        this.tempFilePath = tempFilePath;
    }

    @Override
    public double getCpuTemperature() {
        try {
            if (Files.exists(tempFilePath)) {
                String content = Files.readString(tempFilePath).trim();
                return Double.parseDouble(content) / 1000.0;
            }
        } catch (Exception e) {
        }
        return Double.NaN;
    }
}