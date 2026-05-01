package org.k1den.service;

import java.util.Scanner;

public class WindowsTemperatureStrategy implements TemperatureStrategy {
    @Override
    public double getCpuTemperature() {
        try {
            String command = "powershell.exe -NoProfile -Command " +
                    "\"Get-WmiObject -Namespace root\\OpenHardwareMonitor -Class Sensor | " +
                    "Where-Object { $_.SensorType -eq 'Temperature' -and $_.Name -match 'CPU' } | " +
                    "Select-Object -First 1 -ExpandProperty Value\"";

            Process process = Runtime.getRuntime().exec(command);
            process.getOutputStream().close();

            try (Scanner scanner = new Scanner(process.getInputStream())) {
                if (scanner.hasNextDouble()) {
                    return scanner.nextDouble();
                }
            }
        } catch (Exception e) {
        }
        return 0.0;
    }
}