package in.dreamlab.wicm.graphData;

import in.dreamlab.graphite.graphData.IntervalData;
import in.dreamlab.wicm.graphData.rangeMap.UByteBooleanRangeMapWritable;
import in.dreamlab.wicm.types.UByteInterval;
import in.dreamlab.wicm.types.UnsignedByte;

public class UByteBooleanIntervalData extends IntervalData<UnsignedByte, Boolean> {

    public UByteBooleanIntervalData() {
        super(new UByteInterval(), new UByteBooleanRangeMapWritable());
    }

    public UByteBooleanIntervalData(UByteInterval lifespan) {
        super(lifespan, new UByteBooleanRangeMapWritable());
    }

    public UByteBooleanIntervalData(UByteInterval lifespan, UByteBooleanRangeMapWritable propertyMap) {
        super(lifespan, propertyMap);
    }
}
