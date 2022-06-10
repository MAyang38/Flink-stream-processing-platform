package bishe.model;

public class CarCount {
    private Long time;
    private Integer count;

    public CarCount() {
    }

    public CarCount(Long time, Integer count) {
        this.time = time;
        this.count = count;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "CarCount{" +
                "time=" + time +
                ", count=" + count +
                '}';
    }
}
