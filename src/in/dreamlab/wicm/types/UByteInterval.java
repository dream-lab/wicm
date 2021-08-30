package in.dreamlab.wicm.types;

import com.google.common.collect.Range;
import in.dreamlab.graphite.types.DefaultInterval;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UByteInterval extends DefaultInterval<UnsignedByte> {

    public UByteInterval() {
        super(new UnsignedByte(0), new UnsignedByte(0));
    }

    public UByteInterval(UnsignedByte start, UnsignedByte end) {
        super(start, end);
    }

    public UByteInterval(int start, int end) {
        super(new UnsignedByte(start), new UnsignedByte(end));
    }

    public UByteInterval(Range<UnsignedByte> range) {
        super(range);
    }

    public UByteInterval create(UnsignedByte start, UnsignedByte end) {
        return new UByteInterval(start, end);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        start = new UnsignedByte(dataInput.readByte());
        end = new UnsignedByte(dataInput.readByte());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeByte(start.byteValue());
        dataOutput.writeByte(end.byteValue());
    }

    @Override
    public long getLength() {
        if(this.isEmpty())
            return 0;
        else
            return getEnd().longValue() - getStart().longValue();
    }
}