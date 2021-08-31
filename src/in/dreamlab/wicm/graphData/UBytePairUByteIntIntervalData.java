package in.dreamlab.wicm.graphData;

import in.dreamlab.graphite.graphData.IntervalData;
import in.dreamlab.wicm.graphData.rangeMap.UBytePairUByteIntRangeMapWritable;
import in.dreamlab.wicm.types.UByteInterval;
import in.dreamlab.wicm.types.UnsignedByte;
import org.apache.commons.lang3.tuple.Pair;

public class UBytePairUByteIntIntervalData extends IntervalData<UnsignedByte, Pair<UnsignedByte, Integer>> {

    public UBytePairUByteIntIntervalData() {
        super(new UByteInterval(), new UBytePairUByteIntRangeMapWritable());
    }

    public UBytePairUByteIntIntervalData(UByteInterval lifespan) {
        super(lifespan, new UBytePairUByteIntRangeMapWritable());
    }

    public UBytePairUByteIntIntervalData(UByteInterval lifespan, UBytePairUByteIntRangeMapWritable propertyMap) {
        super(lifespan, propertyMap);
    }
}
