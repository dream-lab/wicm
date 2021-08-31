package in.dreamlab.wicm.graphData;

import in.dreamlab.graphite.graphData.IntervalData;
import in.dreamlab.wicm.graphData.rangeMap.UByteIntRangeMapWritable;
import in.dreamlab.wicm.types.UByteInterval;
import in.dreamlab.wicm.types.UnsignedByte;

public class UByteIntIntervalData extends IntervalData<UnsignedByte, Integer> {

    public UByteIntIntervalData() {
        super(new UByteInterval(), new UByteIntRangeMapWritable());
    }

    public UByteIntIntervalData(UByteInterval lifespan) {
        super(lifespan, new UByteIntRangeMapWritable());
    }

    public UByteIntIntervalData(UByteInterval lifespan, UByteIntRangeMapWritable propertyMap) {
        super(lifespan, propertyMap);
    }
}
