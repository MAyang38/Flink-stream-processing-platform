package bishe.function.source;


import bishe.model.Car;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SourceFromMySQL extends RichSourceFunction<Car> {
    PreparedStatement ps;
    private Connection connection;
    //source 是否正在运行
    private boolean isRunning = true;
    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接。
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "select * from location_info;"; // 编写具体逻辑代码
        ps = this.connection.prepareStatement(sql);
    }

    /**
     * 程序执行完毕就可以进行，关闭连接和释放资源的动作了
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) { //关闭连接和释放资源
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }
//////JDBC 部分
    @Override
    public void run(SourceContext<Car> ctx) throws Exception {
        ResultSet resultSet = ps.executeQuery(); // 执行SQL语句返回结果集
        boolean isFirstLine = true;
        long timeDiff = 0;
        long lastEventTs = 0;
        while (isRunning && resultSet.next()) {
//            System.out.println(resultSet.getInt("id") + "  id号     创建时间 " +
//            resultSet.getTimestamp("create_time"));
            //时间的由String数字转换成Date日期
//            String Time= resultSet.getString("time");
//            Long time = Long.valueOf(Time) ;
//            String formatTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(time));
//                        System.out.println("数字时间是" + time + "Format To String(Date):"+formatTime);
            String Time= resultSet.getString("time");
            DateFormat df = new SimpleDateFormat("yyMMddHHmmss");
            Date d = df.parse(Time);
            long dateTime = d.getTime();
//            System.out.println(dateTime);
            Car car = new Car(
                    resultSet.getInt("id"),
                    resultSet.getString("terminal_phone"),
                    resultSet.getInt("warning_flag_field"),
                    resultSet.getInt("status_field"),
                    resultSet.getString("latitude"),
                    resultSet.getString("longitude"),
                    resultSet.getString("elevation"),
                    resultSet.getInt("speed"),
                    resultSet.getInt("direction"),
                    dateTime,
                    resultSet.getInt("is_have_additional_message"),
                    resultSet.getTimestamp("create_time"),
                    resultSet.getString("create_user"),
                    resultSet.getTimestamp("last_update_time"),
                    resultSet.getString("last_update_user"),
                    resultSet.getInt("is_delete")
                    );
            long eventTs = car.getTime();
            if (isFirstLine) {
                // 从第一行数据提取时间戳
                lastEventTs = eventTs;
                isFirstLine = false;
            }
            else {
                ///数据 按时间缓冲 进入
//                System.out.println("此数据时间为"+ eventTs + " 前一刻时间为" + lastEventTs);
                timeDiff = eventTs - lastEventTs;
//                System.out.println("时间差" + timeDiff);
                if (timeDiff > 0 && timeDiff < 5000) {
                    Thread.sleep(timeDiff /10 );
                    lastEventTs = eventTs;
                }
            }
            ctx.collect(car);

        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    private static Connection getConnection() {
        Connection con = null;
        try {
            //加载mysql 驱动
            Class.forName("com.mysql.jdbc.Driver");
            //获取连接
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/bishe?useUnicode=true&characterEncoding=UTF-8", "root", "123456");
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = "+ e.getMessage());
        }
        return con;
    }
}
