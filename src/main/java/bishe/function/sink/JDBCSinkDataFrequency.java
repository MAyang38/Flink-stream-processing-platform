package bishe.function.sink;

import bishe.model.CarDataFrequency;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

public class JDBCSinkDataFrequency extends RichSinkFunction<CarDataFrequency> {

    private Connection conn;
    private PreparedStatement ps;

    //打开
    @Override
    public void open(Configuration parameters) throws Exception {
        //建立mysql连接


//        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bishe", "root", "123456");
       conn = getConnection();

//        String sql = "INSERT INTO car_data_frequency (terminal_phone, time, dataFrequency, maxTimeInterval,minTimeInterval) VALUES(?,?,?,?,?) ;";

        String sql = "INSERT INTO car_data_frequency_copy1 (terminal_phone, time, dataFrequency, maxTimeInterval,minTimeInterval) VALUES(?,?,?,?,?) on duplicate key update time=VALUES(time),dataFrequency=VALUES(dataFrequency),maxTimeInterval=VALUES(maxTimeInterval),minTimeInterval=VALUES(minTimeInterval);";
        ps = conn.prepareStatement(sql);
        System.out.println("自定义sink，open数据库链接 =====");
    }

    //关闭
    @Override
    public void close() throws Exception {
        //关闭mysql连接
        System.out.println("自定义sink，close数据库连接 =====");
        if(conn != null){
            conn.close();
        }
        if(ps != null){
            ps.close();;
        }
    }

    //每个数据流对象过来，调用的方法
    @Override
    public void invoke(CarDataFrequency value, Context context) throws Exception {

        Timestamp s = new Timestamp(value.getTime());
        ps.setString(1,value.getTerminal_phone());
        ps.setTimestamp(2,s);
        ps.setLong(3,value.getDataFrequency());
        ps.setLong(4,value.getMaxTimeInterval());
        ps.setLong(5,value.getMinTimeInterval());
        ps.executeUpdate();
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
