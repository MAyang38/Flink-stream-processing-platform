package bishe.utils;

import bishe.function.processfunction.carCountProcessWindowFunction;
import bishe.function.processfunction.countCarRichFunction;
import bishe.function.sink.JDBCSinkCarCount;
import bishe.function.source.SourceFromMySQL;
import bishe.model.CarCount;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;


public class countCarNumber {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(5000L);
//        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取car数据源
        DataStream<Tuple3<String, Long, String>> carInfo = env.addSource(new SourceFromMySQL()).setParallelism(1)

                .map(new countCarRichFunction())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String,Long,String>>(Time.seconds(5)) {
                    @Override
                    public long extractTimestamp(Tuple3<String,Long,String> car) {
                        return car.f1;
                    }
                });

        //全部数据都在
        SingleOutputStreamOperator<CarCount> res = carInfo.keyBy(s -> s.f2)
                .timeWindow(Time.minutes(2))
                .process(new carCountProcessWindowFunction());


        res.print();
        //sink到数据库
//        res.addSink(new JDBCSinkCarCount()).setParallelism(1);



        env.execute();

    }
}
