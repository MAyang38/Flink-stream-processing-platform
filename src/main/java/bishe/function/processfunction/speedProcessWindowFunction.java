package bishe.function.processfunction;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class speedProcessWindowFunction extends ProcessWindowFunction<Tuple3<String, Long, Integer>, Object, String, TimeWindow> {
//        ValueState<Integer> ;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

    }

    @Override
    public void process(String s, ProcessWindowFunction<Tuple3<String, Long, Integer>, Object, String, TimeWindow>.Context context, Iterable<Tuple3<String, Long, Integer>> elements, Collector<Object> collector) throws Exception {

            int count = 0;
            for(Tuple3<String, Long, Integer> ele : elements){
                count ++;
                if(ele.f2 > 35)
//                System.out.println("当前key 为" + ele.f0 +"数量为"+count +"当前水位线时间"+ context.currentWatermark() + "窗口结束时间" + context.window().getEnd()+ "当前时间为" + ele.f1 + "速度为"+ ele.f2 + "上一刻时间为" + lastTime +"  " + lastspeed);
               System.out.println("当前终端号为为" + ele.f0 + "速度过快 当前水位线时间"+ context.currentWatermark() + "窗口结束时间" + context.window().getEnd()+ "当前时间为" + ele.f1 + "速度为"+ ele.f2 );
            }
    }

}

