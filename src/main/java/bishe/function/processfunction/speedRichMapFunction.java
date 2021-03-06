package bishe.function.processfunction;

import bishe.model.Car;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;


public class speedRichMapFunction extends RichMapFunction<Car, Tuple3<String, Long, Integer>> {
    @Override
    public Tuple3<String, Long, Integer> map(Car car) throws Exception {
//        if(car.getSpeed() > 35)
//            System.out.println(car);
        return Tuple3.of(car.getTerminal_phone(), car.getTime(), car.getSpeed());
    }

}
