package bishe.function.source;

import bishe.model.Car;
import bishe.model.student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SourceFromMySQLstudent extends RichSourceFunction<student> {
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
        System.out.println("open");
        connection = getConnection();
        String sql = "select * from student;"; // 编写具体逻辑代码
        ps = this.connection.prepareStatement(sql);
//        System.out.println(ps);
        System.out.println("open 结束");
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
    public void run(SourceContext<student> ctx) throws Exception {
        System.out.println("开始source");
        ResultSet resultSet = ps.executeQuery(); // 执行SQL语句返回结果集
        System.out.println(resultSet);

        while (isRunning && resultSet.next()) {


            student student1 = new student(
                    resultSet.getInt("id"),
                    resultSet.getString("name"),
                    resultSet.getString("password"),
                    resultSet.getInt("age")
            );
            System.out.println(student1);
            Thread.sleep(100);
            ctx.collect(student1);

        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    private static Connection getConnection() {
        System.out.println("connection");
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
