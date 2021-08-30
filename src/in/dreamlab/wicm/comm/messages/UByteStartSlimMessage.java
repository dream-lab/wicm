package in.dreamlab.wicm.comm.messages;

import in.dreamlab.graphite.comm.messages.IntervalMessage;
import in.dreamlab.graphite.types.Interval;
import in.dreamlab.wicm.types.UByteInterval;
import in.dreamlab.wicm.types.UnsignedByte;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UByteStartSlimMessage extends IntervalMessage<UnsignedByte, UnsignedByte> {

    public UByteStartSlimMessage() {
        super(new UByteInterval());
    }

    public UByteStartSlimMessage(UnsignedByte start, UnsignedByte payload) {
        super(new UByteInterval(start.intValue(), UnsignedByte.MAX_VALUE), payload);
    }

    public UByteStartSlimMessage(UByteInterval interval, Integer payload) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setValidity(Interval<UnsignedByte> validity) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        validity.setStart(new UnsignedByte(dataInput.readByte()));
        validity.setEnd(new UnsignedByte(UnsignedByte.MAX_VALUE));
        payload = new UnsignedByte(dataInput.readByte());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeByte(validity.getStart().byteValue());
        dataOutput.writeByte(payload.byteValue());
    }

}