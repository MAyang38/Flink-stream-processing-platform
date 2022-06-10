package bishe.model;

public class CarLocationInfo {
    private String terminal;
    private Long time;
    private Double latitude;
    private Double longitude;
    private String area_name;
    private String area_type;
    private Integer count_number;////当前已经运输次数


    public CarLocationInfo() {
    }

    public Integer getCount_number() {
        return count_number;
    }

    public void setCount_number(Integer count_number) {
        this.count_number = count_number;
    }

    public CarLocationInfo(String terminal, Long time, Double latitude, Double longitude, String area_name, String area_type, Integer count_number) {
        this.terminal = terminal;
        this.time = time;
        this.latitude = latitude;
        this.longitude = longitude;
        this.area_name = area_name;
        this.area_type = area_type;
        this.count_number = count_number;
    }

    public String getTerminal() {
        return terminal;
    }

    public void setTerminal(String terminal) {
        this.terminal = terminal;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public String getArea_name() {
        return area_name;
    }

    public void setArea_name(String area_name) {
        this.area_name = area_name;
    }

    public String getArea_type() {
        return area_type;
    }

    public void setArea_type(String area_type) {
        this.area_type = area_type;
    }

    @Override
    public String toString() {
        return "CarLocationInfo{" +
                "terminal='" + terminal + '\'' +
                ", time=" + time +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", area_name='" + area_name + '\'' +
                ", area_type='" + area_type + '\'' +
                ", count_number=" + count_number +
                '}';
    }
}
