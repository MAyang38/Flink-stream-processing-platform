package bishe;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class test {
    public static void main(String[] args) throws SQLException {
//        Date d = new Date();
//        DateFormat format = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z");
//        format.setTimeZone(TimeZone.getTimeZone("GMT"));
//        System.out.println("当前时间转换结果1：" + format.format(d));
//
//        DateFormat df = new SimpleDateFormat("EEE, dd MMM yyyy hh:mm:ss 'GMT'", Locale.US);
//        df.setTimeZone(TimeZone.getTimeZone("GMT"));
//        String date = df.format(new Date());
//        System.out.println("当前时间转换结果2：" + date);

//            resultSet.getTimestamp("create_time"));
        //时间的由String数字转换成Date日期
//            String Time= resultSet.getString("time");
//            Long time = Long.valueOf(220304171313L) ;
//            String formatTime = new SimpleDateFormat("yyMMddHHmmss").format(new Date(time));
//                        System.out.println("数字时间是" + time + "Format To String(Date):"+formatTime);

//        String sDate = "220304171855";
        //1646385595000
        //1646385535000
//        DateFormat df = new SimpleDateFormat("yyMMddHHmmss");

//        try {
//            DateFormat df = new SimpleDateFormat("yyMMddHHmmss");
//            Date d = df.parse(sDate);
//            long dateTime = d.getTime();
//            System.out.println(dateTime);
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
        Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bishe?useUnicode=true&characterEncoding=UTF-8", "root", "123456");
        System.out.println("自定义sink，open数据库链接 =====");
        String sql = "INSERT INTO `car_data_frequency` (`terminal_phone`, `time`, `dataFrequency`, `maxTimeInterval`,`minTimeInterval`) VALUES(?,?,?,?,?);";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1,"11111111111");
        ps.setLong(2,12312412321L);
        ps.setLong(3,12312312231L);
        ps.setLong(4,60000);
        ps.setLong(5,5000);
        ps.executeUpdate();
        if(conn != null) {
            conn.close();
        }
        if(ps != null) {
            ps.close();
        }
    }
}
