package in.dreamlab.wicm.warpOperation;

import in.dreamlab.graphite.warpOperation.Operation;
import in.dreamlab.wicm.types.UnsignedByte;

public class UByteMax extends Operation<UnsignedByte, UnsignedByte> {

    @Override
    public UnsignedByte operate(UnsignedByte prevValue, UnsignedByte currentValue) {
        if(prevValue.compareTo(currentValue) < 0)
            return currentValue;
        return prevValue;
    }

    @Override
    public UnsignedByte createInitValue() {
        return new UnsignedByte(0);
    }
}
