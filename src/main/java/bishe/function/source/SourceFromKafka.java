package bishe.function.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import java.util.Properties;

public class SourceFromKafka {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();

        props.setProperty("bootstrap.servers","master:9092,worker1:9092,worker2:9092");
        props.setProperty("key.deserializer", String.valueOf(String.class));
        props.setProperty("value.deserializer",String.valueOf(String.class));
        props.setProperty("group.id","group");

        env.addSource(new FlinkKafkaConsumer011("topic",new SimpleStringSchema(),props));
    }

}
