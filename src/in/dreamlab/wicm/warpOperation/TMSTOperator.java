package in.dreamlab.wicm.warpOperation;

import in.dreamlab.graphite.warpOperation.Operation;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class TMSTOperator extends Operation<Pair<Integer, Integer>, Pair<Integer, Integer>> {

    @Override
    public Pair<Integer, Integer> operate(Pair<Integer, Integer> prevValue, Pair<Integer, Integer> currentValue) {
        if (currentValue.getRight() < prevValue.getRight()) return currentValue;
        else if(currentValue.getRight().equals(prevValue.getRight())){
            if(currentValue.getLeft() < prevValue.getLeft())
                return currentValue;
        }

        return prevValue;
    }

    @Override
    public Pair<Integer, Integer> createInitValue() {
        return new MutablePair<Integer, Integer>(Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

}
