package bishe.function.processfunction;

import bishe.model.CarSpeedExceed;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Objects;


public class speedProcessWindowFunction extends KeyedProcessFunction<String, Tuple3<String, Long, Integer>, CarSpeedExceed>{

    private ValueState<Long> timerTs;

    private Integer interval;
    public speedProcessWindowFunction(Integer interval) {
        this.interval = interval;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        ValueStateDescriptor<Long> descriptorfirstTime = new ValueStateDescriptor<Long>("timeTs", TypeInformation.of(Long.class));
        timerTs = getRuntimeContext().getState(descriptorfirstTime);
//        timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class));

        super.open(parameters);
    }

    @Override
    public void close() throws Exception {

        super.close();
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, Tuple3<String, Long, Integer>, CarSpeedExceed>.OnTimerContext ctx, Collector<CarSpeedExceed> out) throws Exception {
//        super.onTimer(timestamp, ctx, out);
//        out.collect("传感器" + ctx.getCurrentKey() + "温度值连续" + interval + "s上升");
//        if(Objects.equals(ctx.getCurrentKey(), "11650003676"))
            System.out.println("传感器" + ctx.getCurrentKey() + "时间"+ timestamp +"连续" + interval + "s超速");
            out.collect(new CarSpeedExceed(ctx.getCurrentKey(), timestamp,40));
        timerTs.clear();


    }

    @Override
    public void processElement(Tuple3<String, Long, Integer> ele, KeyedProcessFunction<String, Tuple3<String, Long, Integer>, CarSpeedExceed>.Context ctx, Collector<CarSpeedExceed> collector) throws Exception {

        Long timerts = timerTs.value();
//        if ele.f2 > 40{

//        }
//        for(Tuple3<String, Long, Integer> ele : elements){
//
////                System.out.println(ele.f2);
//                if(ele.f2 > 60) {
////                System.out.println("当前key 为" + ele.f0 +"数量为"+count +"当前水位线时间"+ context.currentWatermark() + "窗口结束时间" + context.window().getEnd()+ "当前时间为" + ele.f1 + "速度为"+ ele.f2 + "上一刻时间为" + lastTime +"  " + lastspeed);
//                    System.out.println("当前终端号为为" + ele.f0 + "速度过快 当前水位线时间" + context.currentWatermark() + "窗口结束时间" + context.window().getEnd() + "当前时间为" + ele.f1 + "速度为" + ele.f2);
//                    out.collect(new CarSpeedExceed(ele.f0, ele.f1, ele.f2));
//        if(Objects.equals(ctx.getCurrentKey(), "11650003676"))
//            System.out.println("当前key为"+ctx.getCurrentKey()+"当前速度为"+ ele.f2+ "定时器内容"+ timerts);
        if( ele.f2 > 40 && timerts == null ){
//            if(Objects.equals(ctx.getCurrentKey(), "11641554082"))
//                System.out.println("创建定时器" + ele.f1  );
            // 计算出定时器时间戳
            Long ts = ctx.timerService().currentWatermark() + interval * 1000L;
            ctx.timerService().registerEventTimeTimer(ts);
            timerTs.update(ts);
        }
        // 如果温度下降，那么删除定时器
        else if( ele.f2 < 40  && timerts != null ){
//            if(Objects.equals(ctx.getCurrentKey(), "11641554082"))
//                System.out.println("删除定时器" + ele.f2 + "时间"+ ele.f1 );
            ctx.timerService().deleteEventTimeTimer(timerts);
            timerTs.clear();
        }



    }
}
//
//public class speedProcessWindowFunction extends ProcessWindowFunction<Tuple3<String, Long, Integer>, CarSpeedExceed, String, TimeWindow> {
////        ValueState<Integer> ;
//private ValueState<Long> timerTs;
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
//        timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Types.LONG));
//    }
//
//    @Override
//    public void process(String s, ProcessWindowFunction<Tuple3<String, Long, Integer>, CarSpeedExceed, String, TimeWindow>.Context ctx, Iterable<Tuple3<String, Long, Integer>> elements, Collector<CarSpeedExceed> out) throws Exception {
//
//            int count = 0;
//        ctx.timerService().registerProcessingTimeTimer(coalescedTime);
//            for(Tuple3<String, Long, Integer> ele : elements){
//                count ++;
////                System.out.println(ele.f2);
//                if(ele.f2 > 60) {
////                System.out.println("当前key 为" + ele.f0 +"数量为"+count +"当前水位线时间"+ context.currentWatermark() + "窗口结束时间" + context.window().getEnd()+ "当前时间为" + ele.f1 + "速度为"+ ele.f2 + "上一刻时间为" + lastTime +"  " + lastspeed);
//                    System.out.println("当前终端号为为" + ele.f0 + "速度过快 当前水位线时间" + context.currentWatermark() + "窗口结束时间" + context.window().getEnd() + "当前时间为" + ele.f1 + "速度为" + ele.f2);
//                    out.collect(new CarSpeedExceed(ele.f0, ele.f1, ele.f2));
//                }
//            }
//    }
//    @Override
//    public void onTimer(long timestamp) throws Exception {
//        super.onTimer(timestamp, ctx, out);
//        out.collect("整数连续1s上升了！");
//        timerTs.clear();
//    }
//
//}

