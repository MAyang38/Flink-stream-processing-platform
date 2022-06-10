package bishe.model;

public class CarDataFrequency {
    private String terminal_phone;
    private Long time;
    private Long dataFrequency;
    private Long maxTimeInterval;
    private Long minTimeInterval;

    public CarDataFrequency() {
    }

    public CarDataFrequency(String terminal_phone, Long time, Long dataFrequency, Long maxTimeInterval, Long minTimeInterval) {
        this.terminal_phone = terminal_phone;
        this.time = time;
        this.dataFrequency = dataFrequency;
        this.maxTimeInterval = maxTimeInterval;
        this.minTimeInterval = minTimeInterval;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public String getTerminal_phone() {
        return terminal_phone;
    }

    public void setTerminal_phone(String terminal_phone) {
        this.terminal_phone = terminal_phone;
    }

    public Long getDataFrequency() {
        return dataFrequency;
    }

    public void setDataFrequency(Long dataFrequency) {
        this.dataFrequency = dataFrequency;
    }

    public Long getMaxTimeInterval() {
        return maxTimeInterval;
    }

    public void setMaxTimeInterval(Long maxTimeInterval) {
        this.maxTimeInterval = maxTimeInterval;
    }

    public Long getMinTimeInterval() {
        return minTimeInterval;
    }

    public void setMinTimeInterval(Long minTimeInterval) {
        this.minTimeInterval = minTimeInterval;
    }

    @Override
    public String toString() {
        return "CarDataFrequency{" +
                "terminal_phone='" + terminal_phone + '\'' +
                ", time=" + time +
                ", dataFrequency=" + dataFrequency +
                ", maxTimeInterval=" + maxTimeInterval +
                ", minTimeInterval=" + minTimeInterval +
                '}';
    }
}
