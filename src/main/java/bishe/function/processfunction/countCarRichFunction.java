package bishe.function.processfunction;

import bishe.model.Car;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class countCarRichFunction extends RichMapFunction<Car, Tuple3<String, Long, String>> {
    @Override
    public Tuple3<String, Long, String> map(Car car) throws Exception {
        return new Tuple3<>(car.getTerminal_phone(), car.getTime(), "1");
    }
}
