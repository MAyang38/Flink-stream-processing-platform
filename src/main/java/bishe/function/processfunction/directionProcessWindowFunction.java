package bishe.function.processfunction;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class directionProcessWindowFunction extends ProcessWindowFunction<Tuple3<String, Long, Integer>, Object, String, TimeWindow> {
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
    public void process(String s, ProcessWindowFunction<Tuple3<String, Long, Integer>, Object, String, TimeWindow>.Context context, Iterable<Tuple3<String, Long, Integer>> elements, Collector<Object> collector) throws Exception {
        Boolean isfirst = true;
//        int lastdirection = direction.value();
//        int lastdirection =context.globalState().getState(descriptor);
        Long LastTime = 0L;
        int count = 0;
        int lastdirection = 0;
        for (Tuple3<String, Long, Integer> ele : elements) {
            count++;

            if (isfirst && lastTime.value() == null) {
                direction.update(ele.f2);
                lastTime.update(ele.f1);
                isfirst = false;
            }
            else{
//              isfirst = false;
                lastdirection = direction.value();
                LastTime = lastTime.value();
                if (Math.abs(ele.f2 - lastdirection) >= 0) {
    //          System.out.println("当前key 为" + ele.f0 +"数量为"+count +"当前水位线时间"+ context.currentWatermark() + "窗口结束时间" + context.window().getEnd()+ "当前时间为" + ele.f1 + "速度为"+ ele.f2 + "上一刻时间为" + lastTime +"  " + lastspeed);
               //筛选终端号 进行测试
                if (ele.f0.equals("11754451997"))
                            System.out.println("警告！！！当前终端号为为" + ele.f0 + "转向过大   " + "当前时间为" + ele.f1 + "方向角度为→" + ele.f2 + "上一刻时间为" + LastTime + " 上一角度 " + lastdirection+ "←当前水位线时间" + context.currentWatermark() + "窗口结束时间" + context.window().getEnd());
                        //          break;
                    }
                lastTime.update(ele.f1);
                direction.update(ele.f2);

//            if (isfirst) {
//                if(direction.value()==null){
//                    direction.update(ele.f2);
////                    lastdirection = direction.value();
//                    lastTime = ele.f1;
//                    isfirst = false;
//                }
//                else {
//                    lastdirection = direction.value();
//                    if (Math.abs(ele.f2 - lastdirection) >= 0) {
////                System.out.println("当前key 为" + ele.f0 +"数量为"+count +"当前水位线时间"+ context.currentWatermark() + "窗口结束时间" + context.window().getEnd()+ "当前时间为" + ele.f1 + "速度为"+ ele.f2 + "上一刻时间为" + lastTime +"  " + lastspeed);
//                        if(ele.f0.equals("11754451997"))
//                            System.out.println("警告！！！当前终端号为为" + ele.f0 + "转向过大 当前水位线时间" + context.currentWatermark() + "窗口结束时间" + context.window().getEnd() + "当前时间为" + ele.f1 + "方向角度为" + ele.f2 + "上一刻时间为" + lastTime + "  " + lastdirection);
//                        //          break;
//                     }
//                }
//            } else {
//                lastdirection = direction.value();
//                if (Math.abs(ele.f2 - lastdirection) >= 0) {
////                System.out.println("当前key 为" + ele.f0 +"数量为"+count +"当前水位线时间"+ context.currentWatermark() + "窗口结束时间" + context.window().getEnd()+ "当前时间为" + ele.f1 + "速度为"+ ele.f2 + "上一刻时间为" + lastTime +"  " + lastspeed);
//                    if(ele.f0.equals("11754451997"))
//                        System.out.println("警告！！！当前终端号为为" + ele.f0 + "转向过大 当前水位线时间" + context.currentWatermark() + "窗口结束时间" + context.window().getEnd() + "当前时间为" + ele.f1 + "方向角度为" + ele.f2 + "上一刻时间为" + lastTime + "  " + lastdirection);
//                    //          break;
//                    //                                    }
//                    direction.update(ele.f2);
//                    lastTime = ele.f1;
//                }
//            }
            }
        }
    }
}