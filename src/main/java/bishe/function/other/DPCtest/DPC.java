package bishe.function.other.DPCtest;

import bishe.model.CarLocationInfo;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class DPC {

    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取Source数据源
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStreamSource<ClusterReading> inputDataStream = env.addSource(new CustomSource.DPCCustomSource());

        DataStreamSink<String> resultDataStream = inputDataStream.keyBy("id")
                .timeWindow(Time.seconds(10L))
                .process(new CustomProcessWindowFunction())
                .print();
        env.execute();
//

//        DataStreamSource<CarLocationInfo> inputDataStream = env.addSource(new CustomSource.DPCCustomSource1());
//        inputDataStream.map(new RichMapFunction<CarLocationInfo, Tuple3<Double,Double,String>>() {
//                    @Override
//                    public Tuple3<Double, Double, String> map(CarLocationInfo car) throws Exception {
//                        return new Tuple3<>(car.getLongitude(),car.getLatitude(),"1");
//                    }
//                })
//                .keyBy(2)
//                .timeWindowAll(Time.seconds(10L))
//                .process(new CustomProcessWindowFunction1())
//                .print();
//        env.execute();

    }
}
