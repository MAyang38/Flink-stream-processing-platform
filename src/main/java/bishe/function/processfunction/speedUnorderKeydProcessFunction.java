package bishe.function.processfunction;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public  class speedUnorderKeydProcessFunction extends KeyedProcessFunction<String, Tuple3<String, Long, Integer>, Object> {
    //定义状态 保存当前车辆速度值
    ValueState<Integer> Speed;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Speed = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("Speed",Integer.class, 0));
    }

    @Override
    public void processElement(Tuple3<String, Long, Integer> value, KeyedProcessFunction<String, Tuple3<String, Long, Integer>, Object>.Context context, Collector<Object> collector) throws Exception {
        if(value.f2 < 40){
            Speed.update(value.f2);
        }
        else {
            System.out.println(value.f0+"车速过快"+"   当前速度为"+ value.f2 +"   时间戳为" + value.f1);
        }
    }


}