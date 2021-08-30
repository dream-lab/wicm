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

public class UByteUByteRangeMapWritable extends RangeMapWritable<UnsignedByte, UnsignedByte> {

    public UByteUByteRangeMapWritable() {
        super(new Char2ObjectOpenHashMap<TreeRangeMap<UnsignedByte, UnsignedByte>>(DEFAULT_INITIAL_SIZE));
    }

    public UByteUByteRangeMapWritable(Char2ObjectOpenHashMap<TreeRangeMap<UnsignedByte, UnsignedByte>> char2ObjectHashMap) {
        super(char2ObjectHashMap);
    }

    public UByteUByteRangeMapWritable(TreeRangeMap<UnsignedByte, UnsignedByte> rangeMap) {
        super(rangeMap);
    }

    public UByteUByteRangeMapWritable(char propertyID, TreeRangeMap<UnsignedByte, UnsignedByte> rangeMap) {
        super(propertyID, rangeMap);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int mapSize = in.readInt(), rangeMapSize;
        while (mapSize > 0) {
            char propertyID = in.readChar();
            TreeRangeMap<UnsignedByte, UnsignedByte> propertyRangeMap = TreeRangeMap.<UnsignedByte, UnsignedByte>create();
            rangeMapSize = in.readInt();
            if (rangeMapSize > 0) {
                UnsignedByte lower, upper, value;
                for (int i = 0; i < rangeMapSize; i++) {
                    lower = new UnsignedByte(in.readByte());
                    upper = new UnsignedByte(in.readByte());
                    value = new UnsignedByte(in.readByte());
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
            for (Map.Entry<Character, TreeRangeMap<UnsignedByte, UnsignedByte>> property : char2ObjectHashMap.entrySet()) {
                out.writeChar(property.getKey());
                if (property.getValue().asMapOfRanges().size() > 0) {
                    out.writeInt(property.getValue().asMapOfRanges().size());
                    for (Map.Entry<Range<UnsignedByte>, UnsignedByte> entry : property.getValue().asMapOfRanges().entrySet()) {
                        out.writeByte(entry.getKey().lowerEndpoint().byteValue());
                        out.writeByte(entry.getKey().upperEndpoint().byteValue());
                        out.writeByte(entry.getValue().byteValue());
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
