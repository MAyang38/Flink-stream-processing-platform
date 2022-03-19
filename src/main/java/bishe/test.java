package bishe;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class test {
    public static void main(String[] args) {
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

        String sDate = "220304171313";

//        DateFormat df = new SimpleDateFormat("yyMMddHHmmss");

        try {
            DateFormat df = new SimpleDateFormat("yyMMddHHmmss");
            Date d = df.parse(sDate);
            long dateTime = d.getTime();
            System.out.println(dateTime);
        } catch (ParseException e) {
            e.printStackTrace();
        }


    }
}
