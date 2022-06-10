package bishe.function.sink;

import bishe.model.student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class JDBCSinkStudent<T> extends RichSinkFunction<T> {
    private Connection conn;
    private PreparedStatement ps;

    //打开
    @Override
    public void open(Configuration parameters) throws Exception {
        //建立mysql连接
        System.out.println("建立连接");
        conn = getConnection();
//            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bishe", "root", "123456");
//        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bishe?useUnicode=true&characterEncoding=UTF-8", "root", "123456");

        String sql = "INSERT INTO studentsink (id, name, password, age) VALUES(?,?,?,?)on duplicate key update name=VALUES(password);";
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
    public void invoke(T value1, Context context) throws Exception {
        student value = (student)value1;
        ps.setInt(1,value.getId());
        ps.setString(2,value.getName());
        ps.setString(3,value.getPassword());
        ps.setInt(4,value.getAge());

        ps.executeUpdate();
    }

    private static Connection getConnection() {
        Connection con = null;
        try {
            //加载mysql 驱动
            Class.forName("com.mysql.jdbc.Driver");
            System.out.println("source开始连接数据库");
            //获取连接
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/bishe", "root", "123456");
            System.out.println("source连接数据库成功");
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = "+ e.getMessage());
        }
        return con;
    }
}
