package bishe.model;


import java.sql.Timestamp;

public class Car {

    public int id;                     //id 号 没有用
    public String terminal_phone;      //终端手机号   唯一确定
    public int warning_flag_field;     //报警标志
    public int status_field;            //车辆状态
    public String latitude;             //纬度，精确到百万分之一度，小数点后六位
    public String longitude;            //经度，精确到百万分之一度，小数点后六位
    public String elevation;            //海拔、高度，单位m
    public int speed;                   //速度，单位0.1km/h
    public int direction;               //方向,0-359,正北为 0,顺时针
    public Long time;                 //上报(采样)时间
    public int is_have_additional_message;//是否有附加信息  0/1
    public Timestamp create_time;       //创建时间
    public String create_user;          //创建人
    public Timestamp last_update_time;  //最近更新时间
    public String last_update_user;     //最后更新人
    public int is_delete;               //是否删除

    public Car(int id, String terminal_phone, int warning_flag_field, int status_field, String latitude, String longitude, String elevation, int speed, int direction, Long time, int is_have_additional_message, Timestamp create_time, String create_user, Timestamp last_update_time, String last_update_user, int is_delete) {
        this.id = id;
        this.terminal_phone = terminal_phone;
        this.warning_flag_field = warning_flag_field;
        this.status_field = status_field;
        this.latitude = latitude;
        this.longitude = longitude;
        this.elevation = elevation;
        this.speed = speed;
        this.direction = direction;
        this.time = time;
        this.is_have_additional_message = is_have_additional_message;
        this.create_time = create_time;
        this.create_user = create_user;
        this.last_update_time = last_update_time;
        this.last_update_user = last_update_user;
        this.is_delete = is_delete;
    }
    public Car(){}
    @Override
    public String toString() {
        return "Car{" +
                "id=" + id +
                ", terminal_phone='" + terminal_phone + '\'' +
                ", warning_flag_field=" + warning_flag_field +
                ", status_field=" + status_field +
                ", latitude='" + latitude + '\'' +
                ", longitude='" + longitude + '\'' +
                ", elevation='" + elevation + '\'' +
                ", speed=" + speed +
                ", direction=" + direction +
                ", time='" + time + '\'' +
                ", is_have_additional_message=" + is_have_additional_message +
                ", create_time=" + create_time +
                ", create_user='" + create_user + '\'' +
                ", last_update_time=" + last_update_time +
                ", last_update_user='" + last_update_user + '\'' +
                ", is_delete=" + is_delete +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getTerminal_phone() {
        return terminal_phone;
    }

    public void setTerminal_phone(String terminal_phone) {
        this.terminal_phone = terminal_phone;
    }

    public int getWarning_flag_field() {
        return warning_flag_field;
    }

    public void setWarning_flag_field(int warning_flag_field) {
        this.warning_flag_field = warning_flag_field;
    }

    public int getStatus_field() {
        return status_field;
    }

    public void setStatus_field(int status_field) {
        this.status_field = status_field;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

    public String getElevation() {
        return elevation;
    }

    public void setElevation(String elevation) {
        this.elevation = elevation;
    }

    public int getSpeed() {
        return speed;
    }

    public void setSpeed(int speed) {
        this.speed = speed;
    }

    public int getDirection() {
        return direction;
    }

    public void setDirection(int direction) {
        this.direction = direction;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public int getIs_have_additional_message() {
        return is_have_additional_message;
    }

    public void setIs_have_additional_message(int is_have_additional_message) {
        this.is_have_additional_message = is_have_additional_message;
    }

    public Timestamp getCreate_time() {
        return create_time;
    }

    public void setCreate_time(Timestamp create_time) {
        this.create_time = create_time;
    }

    public String getCreate_user() {
        return create_user;
    }

    public void setCreate_user(String create_user) {
        this.create_user = create_user;
    }

    public Timestamp getLast_update_time() {
        return last_update_time;
    }

    public void setLast_update_time(Timestamp last_update_time) {
        this.last_update_time = last_update_time;
    }

    public String getLast_update_user() {
        return last_update_user;
    }

    public void setLast_update_user(String last_update_user) {
        this.last_update_user = last_update_user;
    }

    public int getIs_delete() {
        return is_delete;
    }

    public void setIs_delete(int is_delete) {
        this.is_delete = is_delete;
    }
}