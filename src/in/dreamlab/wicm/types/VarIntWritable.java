package in.dreamlab.wicm.types;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class VarIntWritable implements WritableComparable {
    private byte[] value;

    public VarIntWritable() {
    }

    public VarIntWritable(int value) {
        this.set(value);
    }

    public void set(int value) {
        this.value = VariableInteger.encodeUInt32(value);
    }

    public int get() {
        return VariableInteger.decodeUInt32(this.value);
    }

    public boolean equals(Object o) {
        if (!(o instanceof VarIntWritable)) {
            return false;
        } else {
            VarIntWritable other = (VarIntWritable)o;
            return get() == other.get();
        }
    }

    public int hashCode() {
        return Integer.hashCode(get());
    }

    public int compareTo(Object o) {
        int thisValue = get();
        int thatValue = ((VarIntWritable)o).get();
        return thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1);
    }

    public String toString() {
        return Integer.toString(get());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.write(this.value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        byte[] readValue = new byte[5];
        int position = 0;
        readValue[position++] = dataInput.readByte();
        while((readValue[(position-1)] & 128) == 128){
            readValue[position++] = dataInput.readByte();
        }

        value = new byte[position];
        System.arraycopy(readValue, 0, value, 0, position);
    }
}
