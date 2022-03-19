package bishe.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.java.tuple.Tuple3;

public  class MyPeriodicGenerator implements WatermarkGenerator<Tuple3<String, Long, Integer>> {

    private final long maxOutOfOrderness = 5 * 1000; // 5second
    private long currentMaxTimestamp;                 // 已抽取的Timestamp最大值

    @Override
    public void onEvent(Tuple3<String, Long, Integer> event, long eventTimestamp, WatermarkOutput output) {
        // 更新currentMaxTimestamp为当前遇到的最大值
        currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // Watermark比currentMaxTimestamp最大值慢5秒
        output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness));
    }

}
