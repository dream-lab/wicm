package in.dreamlab.wicm.comm.messages;

import in.dreamlab.graphite.comm.messages.IntervalMessage;
import in.dreamlab.graphite.types.IntInterval;
import in.dreamlab.graphite.types.Interval;
import in.dreamlab.wicm.types.UByteInterval;
import in.dreamlab.wicm.types.UnsignedByte;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UByteBooleanStartSlimMessage extends IntervalMessage<UnsignedByte, Boolean> {

    public UByteBooleanStartSlimMessage() {
        super(new UByteInterval());
    }

    public UByteBooleanStartSlimMessage(Integer start, Boolean payload) {
        super(new UByteInterval(start, UnsignedByte.MAX_VALUE), payload);
    }

    public UByteBooleanStartSlimMessage(IntInterval interval, Integer payload) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setValidity(Interval<UnsignedByte> validity) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        validity.setStart(new UnsignedByte(Byte.toUnsignedInt(dataInput.readByte())));
        validity.setEnd(new UnsignedByte(UnsignedByte.MAX_VALUE));
        payload = dataInput.readBoolean();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeByte(validity.getStart().byteValue());
        dataOutput.writeBoolean(payload);
    }

}
