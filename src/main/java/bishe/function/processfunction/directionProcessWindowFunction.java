package bishe.function.processfunction;

import bishe.model.CarDataFrequency;
import bishe.model.CarDirection;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class directionProcessWindowFunction extends ProcessWindowFunction<Tuple3<String, Long, Integer>, CarDirection, String, TimeWindow> {
    //记录当前终端 中 的角度   每个终端都有一个
    private ValueState<Integer> direction;
    private ValueState<Long> lastTime ;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<Integer>("direction", TypeInformation.of(Integer.class), 0);
        direction = getRuntimeContext().getState(descriptor);
        ValueStateDescriptor<Long> descriptorLong = new ValueStateDescriptor<Long>("time", TypeInformation.of(Long.class));
        lastTime = getRuntimeContext().getState(descriptorLong);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void process(String s, ProcessWindowFunction<Tuple3<String, Long, Integer>, CarDirection, String, TimeWindow>.Context context, Iterable<Tuple3<String, Long, Integer>> elements, Collector<CarDirection> out) throws Exception {
        Boolean isfirst = true;
//        int lastdirection = direction.value();
//        int lastdirection =context.globalState().getState(descriptor);
        Long LastTime = 0L;
        int count = 0;
        int lastdirection = 0;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for (Tuple3<String, Long, Integer> ele : elements) {
            count++;
//            System.out.println("count" + count + "终端号"+ ele.f0 );
            if (isfirst && lastTime.value() == null) {
                direction.update(ele.f2);
                lastTime.update(ele.f1);
                isfirst = false;
            }
            else{
                lastdirection = direction.value();
                LastTime = lastTime.value();
                int change = Math.abs(ele.f2 - lastdirection);
                if (change > 200){
                    change = 360 - change;
                }
                if (change>= 100) {

                    Timestamp timestamp = new Timestamp(ele.f1);
//                    System.out.println(timestamp);
//                    String format = simpleDateFormat.format(new Date(ele.f1));
                    out.collect(new CarDirection(ele.f0, timestamp, ele.f2, lastdirection));
//              System.out.println("当前key 为" + ele.f0 +"数量为"+count +"当前水位线时间"+ context.currentWatermark() + "窗口结束时间" + context.window().getEnd()+ "当前时间为" + ele.f1 + "速度为"+ ele.f2 + "上一刻时间为" + lastTime.value() );
               //筛选终端号 进行测试
//                if (ele.f0.equals("11754451997"))
//                            System.out.println("警告！！！当前终端号为为" + ele.f0 + "转向过大   " + "当前时间为" + ele.f1 + "方向角度为→" + ele.f2 + "上一刻时间为" + LastTime + " 上一角度 " + lastdirection+ "←当前水位线时间" + context.currentWatermark() + "窗口结束时间" + context.window().getEnd());
                        //          break;
                    }

                lastTime.update(ele.f1);
                direction.update(ele.f2);

            }
        }
    }
}