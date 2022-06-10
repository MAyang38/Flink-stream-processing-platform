package bishe.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.java.tuple.Tuple4;

public class DistancePeriodicGenerator implements WatermarkGenerator<Tuple4<String, Long, Double, Double>> {

private final long maxOutOfOrderness=40*1000; // 40second
private long currentMaxTimestamp;                 // 已抽取的Timestamp最大值

@Override
public void onEvent(Tuple4<String, Long, Double, Double> event, long eventTimestamp, WatermarkOutput output){
        // 更新currentMaxTimestamp为当前遇到的最大值
        currentMaxTimestamp=Math.max(currentMaxTimestamp,eventTimestamp);
        }

@Override
public void onPeriodicEmit(WatermarkOutput output){
        // Watermark比currentMaxTimestamp最大值慢5秒
        output.emitWatermark(new Watermark(currentMaxTimestamp-maxOutOfOrderness));
        }

        }
