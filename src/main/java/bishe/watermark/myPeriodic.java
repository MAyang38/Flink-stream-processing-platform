package bishe.watermark;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class myPeriodic implements AssignerWithPeriodicWatermarks<Tuple3<String, Long, Integer>> {

    private Long bound = 5 * 1000L; // 延迟一分钟
    private Long maxTs = 0L; // 当前最大时间戳
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
//        System.out.println("发送时间戳" + (maxTs - bound));
        return new Watermark(maxTs - bound);
    }

    @Override
    public long extractTimestamp(Tuple3<String, Long, Integer> element, long l) {
        maxTs = Math.max(maxTs, element.f1);
        return element.f1;

    }
}
