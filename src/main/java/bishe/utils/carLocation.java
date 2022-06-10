package bishe.utils;

import bishe.function.processfunction.clusterRichFunction;
import bishe.function.source.SourceFromMySQL;
import bishe.model.AreaSetting;
import bishe.model.CarLocationInfo;
import bishe.model.Point;
import bishe.watermark.clusterPeriodicGenerator;
import com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class carLocation {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //添加数据源
        SingleOutputStreamOperator<Tuple4<String, Long, Double, Double>> carInfo =
                env.addSource(new SourceFromMySQL())
                .map(new clusterRichFunction())
//        SingleOutputStreamOperator<Tuple4<String, Long, Double, Double>> map = carInfo
                .assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator((context -> new clusterPeriodicGenerator()))
                        .withTimestampAssigner((event, recordTimestamp) -> event.f1)
                );

        //添加地区边界数据源
        DataStreamSource<AreaSetting> areaSetting = env.addSource(new RichSourceFunction<AreaSetting>() {
            PreparedStatement ps;
            private Connection con;
            //source 是否正在运行
            private boolean isRunning = true;

            @Override
            public void open(Configuration parameters) throws Exception {
                con = DriverManager.getConnection("jdbc:mysql://localhost:3306/bishe", "root", "123456");
                ps = con.prepareStatement("select area_name,mine_area,area_type,border_point from area_setting");
            }

            @Override
            public void close() throws Exception {
                super.close();
                if (con != null) { //关闭连接和释放资源
                    con.close();
                }
                if (ps != null) {
                    ps.close();
                }
            }

            @Override
            public void run(SourceContext<AreaSetting> out) throws Exception {
                ResultSet resultSet = ps.executeQuery(); // 执行SQL语句返回结果集
                Double latitude;
                Double longitude;
//                Point point = new Point();
                List<Point> list = new ArrayList<>();
                while (isRunning && resultSet.next()) {
                    //每个key都要清空上一次
                    list.clear();
                    //获取执行结果
                    String area_name = resultSet.getString("area_name");
                    String mine_area = resultSet.getString("mine_area");
                    String area_type = resultSet.getString("area_type");
                    String border_point = resultSet.getString("border_point");


                    //分割区域点
                    String[] border_list = border_point.split(";");
                    for(int i = 0; i < border_list.length; i++){
                        String[] Points = border_list[i].split(",");
                        latitude = Double.valueOf(Points[0]);
                        longitude = Double.valueOf(Points[1]);

                        list.add(new Point(latitude, longitude));
                    }
                    System.out.println(list);
                    out.collect(new AreaSetting(area_name, mine_area, area_type, list));
//                    System.out.println(area_name + " " + mine_area + " " + area_type + " " + border_point);
                }
                //一小时查询一次数据库全量信息
                Thread.sleep(60 * 1000 * 60);
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });


        //数据流分区
        KeyedStream<Tuple4<String, Long, Double, Double>, String> keydStream  = carInfo.keyBy(e -> e.f0);

        //设置广播数据流
        MapStateDescriptor<String, List> mapState = new MapStateDescriptor<>("map-state", String.class, List.class);
        BroadcastStream<AreaSetting> areaSettingBroadcast = areaSetting.broadcast(mapState);


        //数据流与广播流结合处理
        BroadcastConnectedStream<Tuple4<String, Long, Double, Double>, AreaSetting> connect = keydStream.connect(areaSettingBroadcast);

        SingleOutputStreamOperator<Object> objectSingleOutputStreamOperator = connect.process(new KeyedBroadcastProcessFunction<String, Tuple4<String, Long, Double, Double>, AreaSetting, Object>() {
            @Override
            public void processElement(Tuple4<String, Long, Double, Double> carinfo, KeyedBroadcastProcessFunction<String, Tuple4<String, Long, Double, Double>, AreaSetting, Object>.ReadOnlyContext readOnlyContext, Collector<Object> collector) throws Exception {
                Thread.sleep(2000);
                String terminal_phone = carinfo.f0;
                Long time = carinfo.f1;
                Double latitude = carinfo.f2;
                Double longitude = carinfo.f3;
                ReadOnlyBroadcastState<String, List> broadcastState = readOnlyContext.getBroadcastState(mapState);
//                                System.out.println("数据处理流"+broadcastState);
                Iterable<Map.Entry<String, List>> entries = broadcastState.immutableEntries();
//                                Point p1 = new Point(39.47386315244423,106.93828130522411 );
                Point check = new Point(latitude, longitude);
                List<Point> list = new ArrayList<>();
                boolean flag = false;
                for (Map.Entry<String, List> entry : entries) {
                    String key = entry.getKey();
//                                    System.out.println("key = " + key + "value" + entry.getValue());
                    list = entry.getValue();
//                                    System.out.println(list);
                    boolean b = IsInPolygon(check, list);
                    if (b) {
                        flag = true;
                        System.out.println("当前终端号为" + terminal_phone + "当前时间" + time + "此数据点在" + key + " 区域");
//                                        break;
                    }
                }
                System.out.println("当前数据结束");
                if (!flag) {
                    System.out.println("都不在这些区域");
                }
            }

            @Override
            public void processBroadcastElement(AreaSetting areaSetting, KeyedBroadcastProcessFunction<String, Tuple4<String, Long, Double, Double>, AreaSetting, Object>.Context context, Collector<Object> collector) throws Exception {
                BroadcastState<String, List> broadcastState = context.getBroadcastState(mapState);
                String key = areaSetting.getArea_name() + "_" + areaSetting.getArea_type();
                broadcastState.put(key, areaSetting.getBorder_point());

            }
        }).setParallelism(1);
//        SingleOutputStreamOperator<Object> process = objectSingleOutputStreamOperator.timeWindowAll(Time.seconds(10)).process();


//        carInfo.connect(areaSettingBroadcast)
//                        .process(new BroadcastProcessFunction<Tuple4<String, Long, Double, Double>, AreaSetting, CarLocationInfo>() {
//
////                            MapStateDescriptor<String, List> mapState = new MapStateDescriptor<>("map-state", String.class, List.class);/
//                            //遇到问题  数据进来太快，广播流还未放置好状态
//                            @Override
//                            public void processElement(Tuple4<String, Long, Double, Double> carinfo, BroadcastProcessFunction<Tuple4<String, Long, Double, Double>, AreaSetting, CarLocationInfo>.ReadOnlyContext readOnlyContext, Collector<CarLocationInfo> collector) throws Exception {
//                                Thread.sleep(2000);
//                                String terminal_phone = carinfo.f0;
//                                Long time = carinfo.f1;
//                                Double latitude = carinfo.f2;
//                                Double longitude = carinfo.f3;
//                                ReadOnlyBroadcastState<String, List> broadcastState = readOnlyContext.getBroadcastState(mapState);
////                                System.out.println("数据处理流"+broadcastState);
//                                Iterable<Map.Entry<String, List>> entries = broadcastState.immutableEntries();
////                                Point p1 = new Point(39.47386315244423,106.93828130522411 );
//                                Point check = new Point(latitude, longitude);
//                                List<Point> list = new ArrayList<>();
//                                boolean flag = false;
//                                for(Map.Entry<String, List> entry: entries){
//                                    String key = entry.getKey();
////                                    System.out.println("key = " + key + "value" + entry.getValue());
//                                    list = entry.getValue();
////                                    System.out.println(list);
//                                    boolean b = IsInPolygon(check, list);
//                                    if(b){
//                                        flag = true;
//                                        System.out.println("当前终端号为"+terminal_phone+"当前时间"+time+"此数据点在" + key + " 区域");
////                                        break;
//                                    }
//                                }
//                                System.out.println("当前数据结束");
//                                if(!flag){
//                                    System.out.println("都不在这些区域");
//                                }
//                            }
//
//                            @Override
//                            public void processBroadcastElement(AreaSetting areaSetting, BroadcastProcessFunction<Tuple4<String, Long, Double, Double>, AreaSetting, CarLocationInfo>.Context context, Collector<CarLocationInfo> collector) throws Exception {
//                                BroadcastState<String, List> broadcastState = context.getBroadcastState(mapState);
//                                String key = areaSetting.getArea_name() + "_" + areaSetting.getArea_type();
//                                broadcastState.put(key, areaSetting.getBorder_point());
//
//
////                                System.out.println("广播流"+ broadcastState);
////                                System.out.println(key);
////                                System.out.println(broadcastState.get(key));
//
//
//                            }
//                        }).setParallelism(1);


//        areaSetting.print();
        env.execute();
    }

    public static boolean IsInPolygon(Point checkPoint, List<Point> polygonPoints)
    {
        Boolean inside = false;
        int pointCount = polygonPoints.size();
        Point p1, p2;
        for (int i = 0, j = pointCount - 1; i < pointCount; j = i, i++)//第一个点和最后一个点作为第一条线，之后是第一个点和第二个点作为第二条线，之后是第二个点与第三个点，第三个点与第四个点...
        {
            p1 = polygonPoints.get(i);
            p2 = polygonPoints.get(j);
            if (checkPoint.getLongitude() < p2.getLongitude())
            {//p2在射线之上
                if (p1.getLongitude() <= checkPoint.getLongitude())
                {//p1正好在射线中或者射线下方
                    if ((checkPoint.getLongitude() - p1.getLongitude()) * (p2.getLatitude() - p1.getLatitude()) > (checkPoint.getLatitude() - p1.getLatitude()) * (p2.getLongitude() - p1.getLongitude()))//斜率判断,在P1和P2之间且在P1P2右侧
                    {
                        //射线与多边形交点为奇数时则在多边形之内，若为偶数个交点时则在多边形之外。
                        //由于inside初始值为false，即交点数为零。所以当有第一个交点时，则必为奇数，则在内部，此时为inside=(!inside)
                        //所以当有第二个交点时，则必为偶数，则在外部，此时为inside=(!inside)
                        inside = (!inside);
                    }
                }
            }
            else if (checkPoint.getLongitude() < p1.getLongitude())
            {
                //p2正好在射线中或者在射线下方，p1在射线上
                if ((checkPoint.getLongitude() - p1.getLongitude()) * (p2.getLatitude() - p1.getLatitude()) < (checkPoint.getLatitude() - p1.getLatitude()) * (p2.getLongitude() - p1.getLongitude()))//斜率判断,在P1和P2之间且在P1P2右侧
                {
                    inside = (!inside);
                }
            }
        }
        return inside;
    }
}
