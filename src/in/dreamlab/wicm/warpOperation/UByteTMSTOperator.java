package in.dreamlab.wicm.warpOperation;

import in.dreamlab.graphite.warpOperation.Operation;
import in.dreamlab.wicm.types.UnsignedByte;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class UByteTMSTOperator extends Operation<Pair<UnsignedByte, Integer>, Pair<UnsignedByte, Integer>> {

    @Override
    public Pair<UnsignedByte, Integer> operate(Pair<UnsignedByte, Integer> prevValue, Pair<UnsignedByte, Integer> currentValue) {
        if (currentValue.getLeft().compareTo(prevValue.getLeft()) < 0) return currentValue;
        else if(currentValue.getLeft().equals(prevValue.getLeft())){
            if(currentValue.getRight() < prevValue.getRight())
                return currentValue;
        }

        return prevValue;
    }

    @Override
    public Pair<UnsignedByte, Integer> createInitValue() {
        return new MutablePair<UnsignedByte, Integer>(new UnsignedByte(UnsignedByte.MAX_VALUE), Integer.MAX_VALUE);
    }

}
