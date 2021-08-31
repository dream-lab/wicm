package in.dreamlab.wicm.graphData.rangeMap;

import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeMap;
import in.dreamlab.graphite.graphData.rangeMap.RangeMapWritable;
import in.dreamlab.wicm.types.UnsignedByte;
import it.unimi.dsi.fastutil.chars.Char2ObjectOpenHashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class UByteIntRangeMapWritable extends RangeMapWritable<UnsignedByte, Integer> {

    public UByteIntRangeMapWritable() {
        super(new Char2ObjectOpenHashMap<TreeRangeMap<UnsignedByte, Integer>>(DEFAULT_INITIAL_SIZE));
    }

    public UByteIntRangeMapWritable(Char2ObjectOpenHashMap<TreeRangeMap<UnsignedByte, Integer>> char2ObjectHashMap) {
        super(char2ObjectHashMap);
    }

    public UByteIntRangeMapWritable(TreeRangeMap<UnsignedByte, Integer> rangeMap) {
        super(rangeMap);
    }

    public UByteIntRangeMapWritable(char propertyID, TreeRangeMap<UnsignedByte, Integer> rangeMap) {
        super(propertyID, rangeMap);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int mapSize = in.readInt(), rangeMapSize;
        while (mapSize > 0) {
            char propertyID = in.readChar();
            TreeRangeMap<UnsignedByte, Integer> propertyRangeMap = TreeRangeMap.<UnsignedByte, Integer>create();
            rangeMapSize = in.readInt();
            if (rangeMapSize > 0) {
                UnsignedByte lower, upper;
                Integer value;
                for (int i = 0; i < rangeMapSize; i++) {
                    lower = new UnsignedByte(in.readByte());
                    upper = new UnsignedByte(in.readByte());
                    value = in.readInt();
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
            for (Map.Entry<Character, TreeRangeMap<UnsignedByte, Integer>> property : char2ObjectHashMap.entrySet()) {
                out.writeChar(property.getKey());
                if (property.getValue().asMapOfRanges().size() > 0) {
                    out.writeInt(property.getValue().asMapOfRanges().size());
                    for (Map.Entry<Range<UnsignedByte>, Integer> entry : property.getValue().asMapOfRanges().entrySet()) {
                        out.writeByte(entry.getKey().lowerEndpoint().byteValue());
                        out.writeByte(entry.getKey().upperEndpoint().byteValue());
                        out.writeInt(entry.getValue());
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
