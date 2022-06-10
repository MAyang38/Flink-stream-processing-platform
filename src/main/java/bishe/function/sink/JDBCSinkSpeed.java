package bishe.function.sink;

import bishe.model.CarSpeedExceed;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

public class JDBCSinkSpeed extends RichSinkFunction<CarSpeedExceed> {

    private Connection conn;
    private PreparedStatement ps;
    @Override
    public void invoke(CarSpeedExceed value, Context context) throws Exception {
        Timestamp timestamp = new Timestamp(value.getTime());
        ps.setString(1,value.getTerminal_phone());
        ps.setTimestamp(2,timestamp);

        ps.setLong(3,value.getSpeed());

        ps.executeUpdate();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = getConnection();

//        String sql = "INSERT INTO car_data_frequency (terminal_phone, time, dataFrequency, maxTimeInterval,minTimeInterval) VALUES(?,?,?,?,?) ;";

        String sql = "INSERT INTO car_speed_exceed (terminal_phone, time, speed) VALUES(?,?,?) on duplicate key update time=VALUES(time),speed=VALUES(speed);";
        ps = conn.prepareStatement(sql);
        System.out.println("自定义sink，open数据库链接 =====");
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭mysql连接
        System.out.println("自定义sink，close数据库连接 =====");
        if(conn != null){
            conn.close();
        }
        if(ps != null){
            ps.close();;
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
            System.out.println("-----------mysql get connection has exception , msg = "+ e.getMessage());
        }
        return con;
    }
}
