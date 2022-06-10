package bishe.function.other.DPCtest;

import bishe.model.CarLocationInfo;
import bishe.model.student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.*;
import java.util.Random;


public class CustomSource {
    public static void main(String [] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取Source数据源
        DataStreamSource<CarLocationInfo> inputDataStream = env.addSource(new DPCCustomSource1()).setParallelism(1);
        // 打印输出
        inputDataStream.print();
        //执行
        env.execute();


    }
    public static class DPCCustomSource1 extends RichSourceFunction<CarLocationInfo> {
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

            System.out.println("open");
            connection = getConnection();
            String sql = "select * from car_current_location;"; // 编写具体逻辑代码
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

            if (connection != null) { //关闭连接和释放资源
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        }
        //////JDBC 部分
        @Override
        public void run(SourceContext<CarLocationInfo> ctx) throws Exception {
            System.out.println("开始source");
            ResultSet resultSet = ps.executeQuery(); // 执行SQL语句返回结果集
            System.out.println(resultSet);

            while (isRunning && resultSet.next()) {

                Timestamp time = resultSet.getTimestamp("time");
                Long time1 = time.getTime();
                CarLocationInfo car = new CarLocationInfo(
                        resultSet.getString("terminal_phone"),
                        time1,

                        resultSet.getDouble("longitude"),
                        resultSet.getDouble("latitude"),
                        resultSet.getString("area_name"),
                        resultSet.getString("area_type"),
                        resultSet.getInt("count_number")
//                        resultSet.getInt("age")
                );
//                System.out.println(car);
                Thread.sleep(100);
                ctx.collect(car);

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
//

//
    public static class DPCCustomSource implements SourceFunction<ClusterReading> {
        // 定义无线循环，不断产生数据，除非被cancel
        boolean running = true;
        @Override
        public void run(SourceContext<ClusterReading> sourceContext) throws Exception {
            Random random = new Random();
            while (running)
            {
                int a = random.nextInt(3)+1;   //
                for(int i = 0;i<5;i++){
                    if(a == 1)
                    {
                        double x = Math.round((10.00 + random.nextDouble() * 20.00)*100)/100.0; // 范围[10,30]
                        double y = Math.round((10.00 + random.nextDouble() * 20.00)*100)/100.0;// 范围[10,30]
                        sourceContext.collect(new ClusterReading(x,y,a));
                        Thread.sleep(100L);
                    }
                    if(a == 2)
                    {
                        double x = Math.round((3.00 + random.nextDouble() * 25.00)*100)/100.0;  // 范围[3,28]
                        double y = Math.round((3.00 + random.nextDouble() * 25.00)*100)/100.0;  // 范围[3,28]
                        sourceContext.collect(new ClusterReading(x,y,a));
                        Thread.sleep(100L);
                    }
                    if(a == 3)
                    {
                        double x = Math.round((10.00 + random.nextDouble() * 20.00)*100)/100.0;  // 范围[10,30]
                        double y = Math.round((3.00 + random.nextDouble() * 25.00)*100)/100.0;  // 范围[3,28]
                        sourceContext.collect(new ClusterReading(x,y,a));
                        Thread.sleep(100L);
                    }
                }
                Thread.sleep(1000L);


            }

        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
