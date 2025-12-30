package org.k1den.model;

public class DiskUsage {
    public String mountPoint;
    public long total;
    public long free;
    public double usedPercent;

    public DiskUsage() {}

    public DiskUsage(String mountPoint, long total, long free, double usedPercent) {
        this.mountPoint = mountPoint;
        this.total = total;
        this.free = free;
        this.usedPercent = usedPercent;
    }
}
