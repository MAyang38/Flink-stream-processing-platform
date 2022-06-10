package bishe.model;

import java.sql.Timestamp;

public class CarDirection {
    private String terminal_phone;
    private Timestamp time;
    private Integer lastDirection;
    private Integer nowDirection;


    public CarDirection() {
    }

    public CarDirection(String terminal_phone, Timestamp time, Integer lastDirection, Integer nowDirection) {
        this.terminal_phone = terminal_phone;
        this.time = time;
        this.lastDirection = lastDirection;
        this.nowDirection = nowDirection;
    }

    public String getTerminal_phone() {
        return terminal_phone;
    }

    public void setTerminal_phone(String terminal_phone) {
        this.terminal_phone = terminal_phone;
    }

    public Timestamp getTime() {
        return time;
    }

    public void setTime(Timestamp time) {
        this.time = time;
    }

    public Integer getLastDirection() {
        return lastDirection;
    }

    public void setLastDirection(Integer lastDirection) {
        this.lastDirection = lastDirection;
    }

    public Integer getNowDirection() {
        return nowDirection;
    }

    public void setNowDirection(Integer nowDirection) {
        this.nowDirection = nowDirection;
    }

    @Override
    public String toString() {
        return "CarDirection{" +
                "terminal_phone='" + terminal_phone + '\'' +
                ", time=" + time +
                ", lastDirection=" + lastDirection +
                ", nowDirection=" + nowDirection +
                '}';
    }
}
