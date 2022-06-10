
package bishe.function.processfunction;

import bishe.model.CarDataFrequency;
import org.apache.commons.math3.util.RandomPivotingStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class dataFrequencyProcessWindowFunction extends ProcessWindowFunction<Tuple3<String, Long, Integer>, CarDataFrequency, String, TimeWindow> {
    //记录当前车辆数据个数
    private ValueState<Integer> count;
    //相邻数据最大和最小时间间隔
    private ValueState<Long> maxTimeInterval;
    private ValueState<Long> minTimeInterval;
    //车辆的数据频率
    private ValueState<Long> dataFrequency;
    //记录上一条数据的时间
    private ValueState<Long> lastTime;
    //记录第一条数据的时间
    private ValueState<Long> firstTime;
    private ValueState<Boolean> isfirst ;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //加载状态数据
        ValueStateDescriptor<Integer> descriptorCount = new ValueStateDescriptor<Integer>("direction", TypeInformation.of(Integer.class),0);
        count = getRuntimeContext().getState(descriptorCount);
        ValueStateDescriptor<Long> descriptorMaxTimeInterval = new ValueStateDescriptor<Long>("maxTimeInterval", TypeInformation.of(Long.class));
        maxTimeInterval = getRuntimeContext().getState(descriptorMaxTimeInterval);
        ValueStateDescriptor<Long> descriptorMinTimeInterval = new ValueStateDescriptor<Long>("minTimeInterval", TypeInformation.of(Long.class));
        minTimeInterval = getRuntimeContext().getState(descriptorMinTimeInterval);
        ValueStateDescriptor<Long> descriptorDataFrequency = new ValueStateDescriptor<Long>("dataFrequency", TypeInformation.of(Long.class));
        dataFrequency = getRuntimeContext().getState(descriptorDataFrequency);
        ValueStateDescriptor<Long> descriptorlastTime = new ValueStateDescriptor<Long>("lastTime", TypeInformation.of(Long.class));
        lastTime = getRuntimeContext().getState(descriptorlastTime);
        ValueStateDescriptor<Long> descriptorfirstTime = new ValueStateDescriptor<Long>("firstTime", TypeInformation.of(Long.class));
        firstTime = getRuntimeContext().getState(descriptorfirstTime);
        ValueStateDescriptor<Boolean> descriptorIsFirst = new ValueStateDescriptor<Boolean>("isFirst", TypeInformation.of(Boolean.class),Boolean.TRUE);
        isfirst = getRuntimeContext().getState(descriptorIsFirst);

    }

    @Override
    public void close() throws Exception {
        super.close();
    }
    //窗口处理函数
    @Override
    public void process(String s, ProcessWindowFunction<Tuple3<String, Long, Integer>, CarDataFrequency, String, TimeWindow>.Context context, Iterable<Tuple3<String, Long, Integer>> elements, Collector<CarDataFrequency> out) throws Exception {
        //定义上一条数据时间和当前数据时间
        Long LastTime = 0L;
        Long curTime = 0L;
        //定义数据时间间隔
        Long timeInterval;
//        if(s.equals("11754451997"))
//            System.out.println(s +  "窗口结束时间" + context.window().getEnd()+ "水位线时间"+ context.currentWatermark()+ "窗口数量" + count.value() + "是否是第一个" + isfirst.value());
        for (Tuple3<String, Long, Integer> ele : elements) {


            if (isfirst.value() && count.value() == 0) {
//                System.out.println("cout的值"+ count.value());
                if(s.equals("11754451997"))
                    System.out.println("第一次数据"+"cout的值"+ count.value());
                //更新第一条数据时间  初始化数据
                count.update(count.value() + 1);
                firstTime.update(ele.f1);
                lastTime.update(ele.f1);
                maxTimeInterval.update(0L);
                minTimeInterval.update(10000000000L);
                dataFrequency.update(0L);
                isfirst.update(false);
            }
            else{
//                System.out.println("count" + count.value());
                count.update(count.value() + 1);
                //获取上一条时间
                LastTime = lastTime.value();
                curTime = ele.f1;
                timeInterval = curTime - LastTime;
                //更新最大最小数据间隔
                if (timeInterval > maxTimeInterval.value()){
                    maxTimeInterval.update(timeInterval);
                }
                if(timeInterval < minTimeInterval.value() && timeInterval >=5000){
                    if(timeInterval > 0)
                        minTimeInterval.update(timeInterval);
                }
                //更新车辆数据频率    总时间/ 数据个数

                Long fre = (ele.f1 - firstTime.value()) / count.value();
                if (fre < 5000){
                    fre = 5000L ;
                }
                dataFrequency.update((ele.f1 - firstTime.value()) / count.value());
                    //          System.out.println("当前key 为" + ele.f0 +"数量为"+count +"当前水位线时间"+ context.currentWatermark() + "窗口结束时间" + context.window().getEnd()+ "当前时间为" + ele.f1 + "速度为"+ ele.f2 + "上一刻时间为" + lastTime +"  " + lastspeed);
                    //筛选终端号 进行测试
                if (ele.f0.equals("11642023612"))
                        System.out.println("当前终端号为为" + ele.f0 + "当前数据个数为"+ count.value() + "当前时间为" + ele.f1 + "数据频率→" + dataFrequency.value() + "上一刻时间为" + LastTime + " 最大时间间隔 " + maxTimeInterval.value()+ "最小时间间隔"+ minTimeInterval.value()+ "←当前水位线时间" + context.currentWatermark() + "窗口结束时间" + context.window().getEnd());
            //    out.collect(new CarDataFrequency(ele.f0, ele.f1, dataFrequency.value().longValue(),maxTimeInterval.value().longValue(),minTimeInterval.value().longValue()));

            }
                out.collect(new CarDataFrequency(ele.f0, ele.f1, dataFrequency.value().longValue(),maxTimeInterval.value().longValue(),minTimeInterval.value().longValue()));
                //更新此数据为上一条数据时间
                lastTime.update(ele.f1);

            }
    }


}
