package in.dreamlab.wicm.graphData;

import in.dreamlab.graphite.graphData.IntervalData;
import in.dreamlab.wicm.graphData.rangeMap.UByteUByteRangeMapWritable;
import in.dreamlab.wicm.types.UByteInterval;
import in.dreamlab.wicm.types.UnsignedByte;

public class UByteUByteIntervalData extends IntervalData<UnsignedByte, UnsignedByte> {

    public UByteUByteIntervalData() {
        super(new UByteInterval(), new UByteUByteRangeMapWritable());
    }

    public UByteUByteIntervalData(UByteInterval lifespan) {
        super(lifespan, new UByteUByteRangeMapWritable());
    }

    public UByteUByteIntervalData(UByteInterval lifespan, UByteUByteRangeMapWritable propertyMap) {
        super(lifespan, propertyMap);
    }
}
