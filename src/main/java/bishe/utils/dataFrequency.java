package bishe.utils;

import bishe.function.processfunction.dataFrequencyProcessWindowFunction;
import bishe.function.processfunction.directionRichMapFunction;
import bishe.function.sink.JDBCSinkDataFrequency;

import bishe.function.source.SourceFromMySQL;

import bishe.model.Car;
import bishe.model.CarDataFrequency;
import bishe.watermark.myPeriodic;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class dataFrequency {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置水位线定期发射 五秒一次
        env.getConfig().setAutoWatermarkInterval(10000L);
        //设置事务时间
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //source 数据源
        DataStreamSource<Car> carDataStreamSource = env.addSource(new SourceFromMySQL());


        //map 提取重要信息
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> map = carDataStreamSource
                .map(new directionRichMapFunction())
                        .assignTimestampsAndWatermarks(new myPeriodic());
                //f分配时间戳和水位线
//                .assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator((context -> new MyPeriodicGenerator()))
//                        .withTimestampAssigner((event, recordTimestamp) -> event.f1)
//                );

        DataStream<CarDataFrequency> result = map.keyBy(s -> s.f0)
                //分成20秒的滚动窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(20)))//Timewindow(Time.seconds(10))
//                .allowedLateness(Time.seconds(5))
                //watermark 触发窗口处理函数
                .process(new dataFrequencyProcessWindowFunction());
        result.print();
        result.addSink(new JDBCSinkDataFrequency());

        env.execute("direction");
    }
}
