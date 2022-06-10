package bishe.function.processfunction;

import bishe.function.other.AMapUtils;
import bishe.model.CarDistance;
import bishe.model.LngLat;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class distanceProcessWindowFunction extends ProcessWindowFunction<Tuple4<String, Long, Double, Double>, CarDistance, String, TimeWindow> {
    //记录当前车辆已经走的距离
    private ValueState<Double> distance;
    //记录上一条数据的经纬度
    private ValueState<Double> lastLatitude;
    private ValueState<Double> lastLongitude;

    private ValueState<Long> lastTime;
    //记录第一条数据的时间
    private ValueState<Long> firstTime;
    private ValueState<Boolean> isfirst ;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //加载状态数据   总距离，上一条数据的经纬度
        ValueStateDescriptor<Double> descriptorCount = new ValueStateDescriptor<Double>("direction", TypeInformation.of(Double.class));
        distance = getRuntimeContext().getState(descriptorCount);
        ValueStateDescriptor<Double> descriptorLastLatitude = new ValueStateDescriptor<Double>("lastLatitude", TypeInformation.of(Double.class));
        lastLatitude = getRuntimeContext().getState(descriptorLastLatitude);
        ValueStateDescriptor<Double> descriptorLastLongitude = new ValueStateDescriptor<Double>("LastLongitude", TypeInformation.of(Double.class));
        lastLongitude = getRuntimeContext().getState(descriptorLastLongitude);
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
    public void process(String s, ProcessWindowFunction<Tuple4<String, Long, Double, Double>, CarDistance, String, TimeWindow>.Context context, Iterable<Tuple4<String, Long, Double, Double>> elements, Collector<CarDistance> out) throws Exception {
        //定义上一条数据时间和当前数据时间
        Double LastLatitude = 0D;
        Double curLatitude = 0D;
        Double LastLongitude = 0D;
        Double curLongitude = 0D;
        Double thisDistance = 0D;
        //定义数据时间间隔
        Long timeInterval;

        for (Tuple4<String, Long, Double, Double> ele : elements) {


            if (isfirst.value()) {
                //更新第一条数据时间  初始化数据
                distance.update(0D);
                lastLatitude.update(ele.f2);
                lastLongitude.update(ele.f3);
                if(s.equals("11754451997"))
                    System.out.println("第一条数据 " + lastLatitude.value()+ lastLongitude.value());
                firstTime.update(ele.f1);
                lastTime.update(ele.f1);
                isfirst.update(false);
            }
            else{
//                if(s.equals("11754451997"))
//                    System.out.println("第一条数据 " + lastLatitude.value()+ lastLongitude.value());
                //记录上一次和这一次数据
                LastLatitude = lastLatitude.value();
                LastLongitude = lastLongitude.value();
                curLatitude = ele.f2;
                curLongitude = ele.f3;
//                System.out.println("上一个经纬度"+ LastLongitude + " " + LastLatitude + "此次经纬度" + curLongitude + " " + curLatitude);
                //计算两个经纬度之间的距离
                LngLat start = new LngLat(LastLongitude, LastLatitude);
                LngLat end = new LngLat(curLongitude, curLatitude);
                thisDistance = AMapUtils.calculateLineDistance(start, end);
                //更新距离

                distance.update(distance.value() + thisDistance);
//                lastTime.update(ele.f1);
                //          System.out.println("当前key 为" + ele.f0 +"数量为"+count +"当前水位线时间"+ context.currentWatermark() + "窗口结束时间" + context.window().getEnd()+ "当前时间为" + ele.f1 + "速度为"+ ele.f2 + "上一刻时间为" + lastTime +"  " + lastspeed);
                //筛选终端号 进行测试
                if (ele.f0.equals("11754451997"))
                    System.out.println("当前终端号为为" + ele.f0 +"上一个经纬度"+ LastLongitude + " " + LastLatitude + "此次经纬度" + curLongitude + " " + curLatitude+  "当前总距离为"+ distance.value()+ "此数据走的距离为" + thisDistance + "当前时间为" + ele.f1 + "上一刻时间为" + lastTime.value() + "←当前水位线时间" + context.currentWatermark() + "窗口结束时间" + context.window().getEnd());
                //          break;
                out.collect(new CarDistance(ele.f0, ele.f1, distance.value()));
                lastLatitude.update(curLatitude);
                lastLongitude.update(curLongitude);
            }
            //更新此数据为上一条数据时间
            lastTime.update(ele.f1);

        }

    }


}
