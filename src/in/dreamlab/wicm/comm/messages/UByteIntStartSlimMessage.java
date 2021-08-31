package in.dreamlab.wicm.comm.messages;

import in.dreamlab.graphite.comm.messages.IntervalMessage;
import in.dreamlab.graphite.types.Interval;
import in.dreamlab.wicm.types.UByteInterval;
import in.dreamlab.wicm.types.UnsignedByte;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UByteIntStartSlimMessage extends IntervalMessage<UnsignedByte, Integer> {

    public UByteIntStartSlimMessage() {
        super(new UByteInterval());
    }

    public UByteIntStartSlimMessage(UnsignedByte start, Integer payload) {
        super(new UByteInterval(start.intValue(), UnsignedByte.MAX_VALUE), payload);
    }

    public UByteIntStartSlimMessage(UByteInterval interval, Integer payload) {
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
        payload = org.apache.hadoop.io.file.tfile.Utils.readVInt(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeByte(validity.getStart().byteValue());
        org.apache.hadoop.io.file.tfile.Utils.writeVInt(dataOutput, payload);
    }
}
