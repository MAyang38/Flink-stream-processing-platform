package bishe.utils;


import bishe.function.sink.JDBCSinkStudent;
import bishe.function.source.SourceFromMySQLstudent;

import bishe.model.student;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class sinkToMysqlTest {



    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置水位线定期发射 五秒一次
//        env.getConfig().setAutoWatermarkInterval(10000L);
        //设置事务时间
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        //source 数据源
        DataStreamSource<student> stuDataStreamSource = env.addSource(new SourceFromMySQLstudent());

        stuDataStreamSource.print();
        //map 提取重要信息
        stuDataStreamSource.addSink(new JDBCSinkStudent());


        env.execute("distance");
    }


}
