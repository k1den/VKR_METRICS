package org.k1den.model;

import java.util.List;
import java.util.Map;

public class Metric {
    public String hostname;
    public long timestamp; // epoch ms
    public double cpuLoad; // 0..100
    public double systemLoadAverage;
    public double memoryUsedPercent;
    public long memoryTotal;
    public long memoryAvailable;
    public List<DiskUsage> disks;
    public long networkRxBytes;
    public long networkTxBytes;
    public int processCount;
    public double cpuTemperature; // новое поле
    public Map<String,String> tags;

    public Metric() {}
}


