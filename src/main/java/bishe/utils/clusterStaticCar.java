package bishe.utils;

import bishe.function.processfunction.clusterRichFunction;
import bishe.function.processfunction.directionRichMapFunction;
import bishe.function.sink.JDBCSinkCarLocation;
import bishe.function.source.SourceFromMySQL;
import bishe.model.Car;
import bishe.watermark.MyPeriodicGenerator;
import bishe.watermark.clusterPeriodicGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class clusterStaticCar {
    public static void main(String[] args)  throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(5000L);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //source 数据源

        DataStreamSource<Car> carDataStreamSource = env.addSource(new SourceFromMySQL()).setParallelism(1);
//        carDataStreamSource.print();
        //map 提取重要信息    过滤车速为0的车辆进行聚类
        SingleOutputStreamOperator<Tuple4<String, Long, Double, Double>> map = carDataStreamSource
                .filter(car -> car.getSpeed()==0)
                .map(new clusterRichFunction())
                //f分配时间戳和水位线
                .assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator((context -> new clusterPeriodicGenerator()))
                        .withTimestampAssigner((event, recordTimestamp) -> event.f1)
                );

//        map.print();

//        SingleOutputStreamOperator<Tuple4<String, Long, Double, Double>> res = map.keyBy(0).timeWindow(Time.seconds(20)).process(new ProcessWindowFunction<Tuple4<String, Long, Double, Double>, Tuple4<String, Long, Double, Double>, Tuple, TimeWindow>() {
//            @Override
//            public void process(Tuple tuple, ProcessWindowFunction<Tuple4<String, Long, Double, Double>, Tuple4<String, Long, Double, Double>, Tuple, TimeWindow>.Context context, Iterable<Tuple4<String, Long, Double, Double>> iterable, Collector<Tuple4<String, Long, Double, Double>> out) throws Exception {
//                    for(Tuple4<String, Long, Double, Double> ele : iterable){
//                        out.collect(ele);
//                    }
//            }
//        });
//        res.print();
//        res.addSink(new JDBCSinkCarLocation()).setParallelism(1);

        env.execute();
    }
}
