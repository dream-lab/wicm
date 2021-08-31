package in.dreamlab.wicm.comm.messages;

import in.dreamlab.graphite.comm.messages.IntervalMessage;
import in.dreamlab.graphite.types.IntInterval;
import in.dreamlab.graphite.types.Interval;
import in.dreamlab.wicm.types.UByteInterval;
import in.dreamlab.wicm.types.UnsignedByte;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UByteIntEndSlimMessage extends IntervalMessage<UnsignedByte, Integer> {

    public UByteIntEndSlimMessage() {
        super(new UByteInterval());
    }

    public UByteIntEndSlimMessage(Integer end, Integer payload) {
        super(new UByteInterval(0, end), payload);
    }

    public UByteIntEndSlimMessage(IntInterval interval, Integer payload) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setValidity(Interval<UnsignedByte> validity) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        validity.setStart(new UnsignedByte(0));
        validity.setEnd(new UnsignedByte(Byte.toUnsignedInt(dataInput.readByte())));
        payload = dataInput.readInt();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeByte(validity.getEnd().byteValue());
        dataOutput.writeInt(payload);
    }

}
