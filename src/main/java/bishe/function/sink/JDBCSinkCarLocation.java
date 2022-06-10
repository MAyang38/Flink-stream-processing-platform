package bishe.function.sink;

import bishe.model.CarCount;
import bishe.model.CarLocationInfo;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

public class JDBCSinkCarLocation extends RichSinkFunction<CarLocationInfo> {

    private Connection conn;
    private PreparedStatement ps;

    @Override
    public void invoke(CarLocationInfo value, SinkFunction.Context context) throws Exception {
        Timestamp timestamp = new Timestamp(value.getTime());
//        System.out.println(value.f2 + value.f3);
        ps.setString(1, value.getTerminal());
        ps.setTimestamp(2, timestamp);
        ps.setDouble(3,value.getLongitude());
        ps.setDouble(4,value.getLatitude());
        ps.setString(5,value.getArea_name());
        ps.setString(6,value.getArea_type());
        ps.setInt(7,value.getCount_number());

        ps.executeUpdate();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
//        conn = getConnection();
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bishe", "root", "123456");

//        String sql = "INSERT INTO car_data_frequency (terminal_phone, time, dataFrequency, maxTimeInterval,minTimeInterval) VALUES(?,?,?,?,?) ;";

        String sql = "INSERT INTO car_current_location (terminal_phone, time, longitude, latitude, area_name, area_type, count_number) VALUES(?,?,?,?,?,?,?) on duplicate key update time=VALUES(time),longitude=VALUES(longitude), latitude=VALUES(latitude),area_name=VALUES(area_name), area_type=VALUES(area_type), count_number=VALUES(count_number) ;";
        ps = conn.prepareStatement(sql);
        System.out.println("自定义sink，open数据库链接 =====");
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭mysql连接
        System.out.println("自定义sink，close数据库连接 =====");
        if (conn != null) {
            conn.close();
        }
        if (ps != null) {
            ps.close();
            ;
        }
    }

    private static Connection getConnection() {
        Connection con = null;
        try {
            //加载mysql 驱动
            Class.forName("com.mysql.jdbc.Driver");
            System.out.println("sink开始连接数据库");
            //获取连接
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/bishe", "root", "123456");
            System.out.println("sink连接数据库成功");
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;
    }
}
