package bishe.model;

public class CarSpeedExceed {
    private String terminal_phone;
    private Long time;
    private Integer speed;

    public CarSpeedExceed(String terminal_phone, Long time, Integer speed) {
        this.terminal_phone = terminal_phone;
        this.time = time;
        this.speed = speed;
    }

    public CarSpeedExceed() {
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

    public Integer getSpeed() {
        return speed;
    }

    public void setSpeed(Integer speed) {
        this.speed = speed;
    }

    @Override
    public String toString() {
        return "CarSpeedExceed{" +
                "terminal_phone='" + terminal_phone + '\'' +
                ", time=" + time +
                ", speed=" + speed +
                '}';
    }
}
