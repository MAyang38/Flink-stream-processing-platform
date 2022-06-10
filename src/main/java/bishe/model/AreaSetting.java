package bishe.model;


import java.awt.geom.Point2D;
import java.util.List;

public class AreaSetting {
    //键
    private String area_name;
    private String mine_area;
    private String area_type;

    private List<Point> border_point;

    public AreaSetting() {

    }

    public AreaSetting(String area_name, String mine_area, String area_type, List<Point> border_point) {
        this.area_name = area_name;
        this.mine_area = mine_area;
        this.area_type = area_type;
        this.border_point = border_point;
    }

    public String getArea_name() {
        return area_name;
    }

    public void setArea_name(String area_name) {
        this.area_name = area_name;
    }

    public String getMine_area() {
        return mine_area;
    }

    public void setMine_area(String mine_area) {
        this.mine_area = mine_area;
    }

    public String getArea_type() {
        return area_type;
    }

    public void setArea_type(String area_type) {
        this.area_type = area_type;
    }

    public List<Point> getBorder_point() {
        return border_point;
    }

    public void setBorder_point(List<Point> border_point) {
        this.border_point = border_point;
    }

    @Override
    public String toString() {
        return "AreaSetting{" +
                "area_name='" + area_name + '\'' +
                ", mine_area='" + mine_area + '\'' +
                ", area_type='" + area_type + '\'' +
                ", boder_point=" + border_point +
                '}';
    }
}
