package bishe.function.processfunction;

import bishe.model.CarCount;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.checkerframework.checker.units.qual.C;

import java.util.HashSet;

public class carCountProcessWindowFunction extends ProcessWindowFunction<Tuple3<String, Long, String>, CarCount, String, TimeWindow> {
    @Override
    public void process(String s, ProcessWindowFunction<Tuple3<String, Long, String>, CarCount, String, TimeWindow>.Context context, Iterable<Tuple3<String, Long, String>> iterable, Collector<CarCount> out) throws Exception {
        //使用set集合计算这个窗口中有多少辆车
        HashSet<String> hashSet = new HashSet();
        Integer count= 0;
        for(Tuple3<String, Long, String> el : iterable){
            String terminal_phone = el.f0;
            if(!hashSet.contains(terminal_phone)){
                hashSet.add(terminal_phone);
                count++;
            }
//            System.out.println(el.f0+ " " +el.f1 + "   " +  context.window().getEnd());

        }
        //窗口结束时间
        long end = context.window().getEnd();
        out.collect(new CarCount(end, count));
        System.out.println(context.window().getEnd()+ "    "+count);
    }
}
