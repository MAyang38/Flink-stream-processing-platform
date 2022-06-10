package bishe.utils;

import bishe.function.processfunction.speedProcessWindowFunction;
import bishe.function.processfunction.speedRichMapFunction;
import bishe.function.sink.JDBCSinkSpeed;
import bishe.model.Car;
import bishe.function.source.*;
import bishe.model.CarSpeedExceed;
import bishe.watermark.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


public class speedExceed {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置水位线定期发射 五秒一次
        env.getConfig().setAutoWatermarkInterval(5000L);
        env.setParallelism(1);
        //设置事务时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //source 数据源
        DataStreamSource<Car> carDataStreamSource = env.addSource(new SourceFromMySQL());

//        carDataStreamSource.print();
        //map 提取重要信息
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> map = carDataStreamSource
                .map(new speedRichMapFunction())
                .filter(ele -> ele.f2 > 30)
                //f分配时间戳和水位线
                .assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator((context -> new MyPeriodicGenerator()))
                        .withTimestampAssigner((event, recordTimestamp) -> event.f1)
                );
//         map.print();
          DataStream<CarSpeedExceed> result = map.keyBy(s -> s.f0)
                //分成20秒的滚动窗口
//                .window(TumblingEventTimeWindows.of(Time.seconds(20)))//Timewindow(Time.seconds(10))
                //watermark 触发窗口处理函数
                .process(
                        new speedProcessWindowFunction(30));
//                });
         ///未定义时间窗口的处理办法
        //.process(new MyKeydProcessFunction());
//        result.print();
        result.addSink(new JDBCSinkSpeed());
        ////    ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓Kafka部分↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
//        Properties props = new Properties();
//        props.put("bootstrap.servers", "localhost:9092");
//        props.put("zookeeper.connect", "localhost:2181");
//        props.put("group.id", "metric-group");
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
////        props.put("auto.offset.reset", "earliest");
//        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>(
//                "location_info",   //这个 kafka topic 需要和上面的工具类的 topic 一致
//                new SimpleStringSchema(),
//                props);
//        consumer.setStartFromEarliest();
        //////// ↑↑↑ ↑↑↑ ↑↑↑ ↑↑↑/Kafka部分 ↑↑↑ ↑↑↑ ↑↑↑ ↑↑↑ ↑↑↑

        env.execute("Flink add sink");
    }



}