package org.k1den.service;

import org.junit.jupiter.api.Test;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class LinuxTemperatureStrategyTest {

    @Test
    void testTemperatureParsing() throws Exception {
        Path mockTempFile = Files.createTempFile("mock_temp", ".txt");
        Files.writeString(mockTempFile, "45000\n");

        try {
            LinuxTemperatureStrategy strategy = new LinuxTemperatureStrategy(mockTempFile);

            double temp = strategy.getCpuTemperature();

            assertEquals(45.0, temp, 0.01, "Стратегия должна корректно делить значение на 1000");
        } finally {
            Files.deleteIfExists(mockTempFile);
        }
    }

    @Test
    void testMissingFileReturnsNaN() {
        Path fakePath = Path.of("/path/that/does/not/exist");
        LinuxTemperatureStrategy strategy = new LinuxTemperatureStrategy(fakePath);

        double temp = strategy.getCpuTemperature();

        assertTrue(Double.isNaN(temp), "Если файла нет, метод должен вернуть NaN");
    }
}