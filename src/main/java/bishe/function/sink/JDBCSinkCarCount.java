package bishe.function.sink;

import bishe.model.CarCount;
import bishe.model.CarSpeedExceed;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class JDBCSinkCarCount extends RichSinkFunction<CarCount> {


    private Connection conn;
    private PreparedStatement ps;

    @Override
    public void invoke(CarCount value, SinkFunction.Context context) throws Exception {

        ps.setLong(1, value.getTime());
        ps.setInt(2, value.getCount());


        ps.executeUpdate();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = getConnection();

//        String sql = "INSERT INTO car_data_frequency (terminal_phone, time, dataFrequency, maxTimeInterval,minTimeInterval) VALUES(?,?,?,?,?) ;";

        String sql = "INSERT INTO car_count (time, count) VALUES(?,?) on duplicate key update count=VALUES(count);";
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