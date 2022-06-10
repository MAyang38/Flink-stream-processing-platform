package bishe.utils;

import bishe.function.processfunction.distanceCalculateRichMapFunction;
import bishe.function.processfunction.distanceProcessWindowFunction;
import bishe.function.sink.JDBCSinkDistance;
import bishe.function.source.SourceFromMySQL;

import bishe.model.Car;
import bishe.model.CarDistance;
import bishe.watermark.DistancePeriodicGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class distanceCalculate {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置水位线定期发射 五秒一次
        env.getConfig().setAutoWatermarkInterval(5000L);
//        env.setParallelism(1);
        //设置事务时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //source 数据源
        DataStreamSource<Car> carDataStreamSource = env.addSource(new SourceFromMySQL()).setParallelism(1);
//        carDataStreamSource.print();
        //map 提取重要信息
        SingleOutputStreamOperator<Tuple4<String, Long, Double, Double>> map = carDataStreamSource
                .map(new distanceCalculateRichMapFunction())
                //f分配时间戳和水位线
                .assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator((context -> new DistancePeriodicGenerator()))
                        .withTimestampAssigner((event, recordTimestamp) -> event.f1)
                );
//            map.print();
        DataStream<CarDistance> result = map.keyBy(s -> s.f0)
                //分成20秒的滚动窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(20)))//Timewindow(Time.seconds(10))
                //watermark 触发窗口处理函数
                .process(new distanceProcessWindowFunction());
        result.print();
//        //将最终距离sink到mysql中

        result.addSink(new JDBCSinkDistance()).setParallelism(1);
        env.execute("distance");
    }
}
