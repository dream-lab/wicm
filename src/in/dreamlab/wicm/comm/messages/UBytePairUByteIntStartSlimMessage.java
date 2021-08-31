package in.dreamlab.wicm.comm.messages;

import in.dreamlab.graphite.comm.messages.IntervalMessage;
import in.dreamlab.graphite.types.Interval;
import in.dreamlab.wicm.types.UByteInterval;
import in.dreamlab.wicm.types.UnsignedByte;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UBytePairUByteIntStartSlimMessage extends IntervalMessage<UnsignedByte, Pair<UnsignedByte, Integer>> {

    public UBytePairUByteIntStartSlimMessage() {
        super(new UByteInterval());
    }

    public UBytePairUByteIntStartSlimMessage(UnsignedByte start, Pair<UnsignedByte, Integer> payload) {
        super(new UByteInterval(start.intValue(), UnsignedByte.MAX_VALUE), payload);
    }

    @Override
    public void setValidity(Interval<UnsignedByte> validity) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        validity.setStart(new UnsignedByte(dataInput.readByte()));
        validity.setEnd(new UnsignedByte(UnsignedByte.MAX_VALUE));
        payload = new MutablePair<UnsignedByte, Integer>(
                new UnsignedByte(dataInput.readByte())
                ,org.apache.hadoop.io.file.tfile.Utils.readVInt(dataInput));
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeByte(validity.getStart().byteValue());
        dataOutput.writeByte(payload.getLeft().byteValue());
        org.apache.hadoop.io.file.tfile.Utils.writeVInt(dataOutput, payload.getRight());
    }
}
