package bishe.model;

public class Point {

    //维度
    private Double latitude;
    private Double longitude;

    public Point() {
    }

    public Point(Double latitude, Double longitude) {
        this.latitude = latitude;
        this.longitude = longitude;
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

//    @Override
//    public String toString() {
//        return "Point{" +
//                "latitude=" + latitude +
//                ", longitude=" + longitude +
//                '}';
//    }
    @Override
    public String toString() {
        return  "new Point("+latitude + "," + longitude  +")";
    }
}