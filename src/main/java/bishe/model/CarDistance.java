package bishe.model;

public class CarDistance {
    private String terminal_phone;
    private Long time;
    private Double distance;

    public CarDistance(String terminal_phone, Long time, Double distance) {

        this.terminal_phone = terminal_phone;
        this.time = time;
        this.distance = distance;
    }

    public CarDistance() {
    }

    public String getTerminal_phone() {
        return terminal_phone;
    }

    public void setTerminal_phone(String terminal_phone) {
        this.terminal_phone = terminal_phone;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public Double getDistance() {
        return distance;
    }

    public void setDistance(Double distance) {
        this.distance = distance;
    }

    @Override
    public String toString() {
        return "CarDistance{" +
                "terminal_phone='" + terminal_phone + '\'' +
                ", time=" + time +
                ", distance=" + distance +
                '}';
    }
}
