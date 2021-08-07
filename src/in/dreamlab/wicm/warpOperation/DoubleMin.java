package in.dreamlab.wicm.warpOperation;

import in.dreamlab.graphite.warpOperation.Operation;

public class DoubleMin extends Operation<Double, Double> {

    @Override
    public Double operate(Double prevValue, Double currentValue) {
        return Double.min(prevValue, currentValue);
    }

    @Override
    public Double createInitValue() {
        return Double.POSITIVE_INFINITY;
    }

}
