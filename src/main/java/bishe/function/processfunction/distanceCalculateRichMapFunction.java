package bishe.function.processfunction;

import bishe.model.Car;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class distanceCalculateRichMapFunction extends RichMapFunction<Car, Tuple4<String, Long, Double, Double>> {
    @Override
    public Tuple4<String, Long, Double, Double> map(Car car) throws Exception {
        return Tuple4.of(car.getTerminal_phone(), car.getTime(), car.getLatitude(), car.getLongitude());
    }
}
