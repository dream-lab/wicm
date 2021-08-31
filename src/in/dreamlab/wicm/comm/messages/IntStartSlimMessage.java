package in.dreamlab.wicm.comm.messages;

import in.dreamlab.graphite.comm.messages.IntervalMessage;
import in.dreamlab.graphite.types.IntInterval;
import in.dreamlab.graphite.types.Interval;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntStartSlimMessage extends IntervalMessage<Integer, Integer> {

    public IntStartSlimMessage() {
        super(new IntInterval());
    }

    public IntStartSlimMessage(Integer start, Integer payload) {
        super(new IntInterval(start, Integer.MAX_VALUE), payload);
    }

    public IntStartSlimMessage(IntInterval interval, Integer payload) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setValidity(Interval<Integer> validity) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        validity.setStart(org.apache.hadoop.io.file.tfile.Utils.readVInt(dataInput));
        validity.setEnd(Integer.MAX_VALUE);
        payload = org.apache.hadoop.io.file.tfile.Utils.readVInt(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        org.apache.hadoop.io.file.tfile.Utils.writeVInt(dataOutput, validity.getStart());
        org.apache.hadoop.io.file.tfile.Utils.writeVInt(dataOutput, payload);
    }

}
