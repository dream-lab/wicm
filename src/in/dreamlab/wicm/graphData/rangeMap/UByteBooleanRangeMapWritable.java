package in.dreamlab.wicm.graphData.rangeMap;

import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeMap;
import in.dreamlab.graphite.graphData.rangeMap.RangeMapWritable;
import in.dreamlab.wicm.types.UnsignedByte;
import it.unimi.dsi.fastutil.chars.Char2ObjectOpenHashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map.Entry;

public class UByteBooleanRangeMapWritable extends RangeMapWritable<UnsignedByte, Boolean> {

    public UByteBooleanRangeMapWritable() {
        super(new Char2ObjectOpenHashMap<TreeRangeMap<UnsignedByte, Boolean>>(DEFAULT_INITIAL_SIZE));
    }

    public UByteBooleanRangeMapWritable(Char2ObjectOpenHashMap<TreeRangeMap<UnsignedByte, Boolean>> char2ObjectHashMap) {
        super(char2ObjectHashMap);
    }

    public UByteBooleanRangeMapWritable(TreeRangeMap<UnsignedByte, Boolean> rangeMap) {
        super(rangeMap);
    }

    public UByteBooleanRangeMapWritable(char propertyID, TreeRangeMap<UnsignedByte, Boolean> rangeMap) {
        super(propertyID, rangeMap);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int mapSize = in.readInt(), rangeMapSize;
        while (mapSize > 0) {
            char propertyID = in.readChar();
            TreeRangeMap<UnsignedByte, Boolean> propertyRangeMap = TreeRangeMap.<UnsignedByte, Boolean>create();
            rangeMapSize = in.readInt();
            if (rangeMapSize > 0) {
                UnsignedByte lower, upper;
                Boolean value;
                for (int i = 0; i < rangeMapSize; i++) {
                    lower = new UnsignedByte(in.readByte());
                    upper = new UnsignedByte(in.readByte());
                    value = in.readBoolean();
                    propertyRangeMap.put(Range.<UnsignedByte>closedOpen(lower, upper), value);
                }
            }
            this.char2ObjectHashMap.put(propertyID, propertyRangeMap);
            mapSize--;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (char2ObjectHashMap.size() > 0) {
            out.writeInt(char2ObjectHashMap.size());
            for (Entry<Character, TreeRangeMap<UnsignedByte, Boolean>> property : char2ObjectHashMap.entrySet()) {
                out.writeChar(property.getKey());
                if (property.getValue().asMapOfRanges().size() > 0) {
                    out.writeInt(property.getValue().asMapOfRanges().size());
                    for (Entry<Range<UnsignedByte>, Boolean> entry : property.getValue().asMapOfRanges().entrySet()) {
                        out.writeByte(entry.getKey().lowerEndpoint().byteValue());
                        out.writeByte(entry.getKey().upperEndpoint().byteValue());
                        out.writeBoolean(entry.getValue());
                    }
                } else {
                    out.writeInt(0);
                }
            }
        } else {
            out.writeInt(0);
        }
    }
}
