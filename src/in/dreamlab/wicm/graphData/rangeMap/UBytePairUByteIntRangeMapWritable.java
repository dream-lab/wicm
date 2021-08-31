package in.dreamlab.wicm.graphData.rangeMap;

import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeMap;
import in.dreamlab.graphite.graphData.rangeMap.RangeMapWritable;
import in.dreamlab.wicm.types.UnsignedByte;
import it.unimi.dsi.fastutil.chars.Char2ObjectOpenHashMap;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map.Entry;

public class UBytePairUByteIntRangeMapWritable extends RangeMapWritable<UnsignedByte, Pair<UnsignedByte, Integer>> {

    public UBytePairUByteIntRangeMapWritable() {
        super(new Char2ObjectOpenHashMap<TreeRangeMap<UnsignedByte, Pair<UnsignedByte, Integer>>>(DEFAULT_INITIAL_SIZE));
    }

    public UBytePairUByteIntRangeMapWritable(Char2ObjectOpenHashMap<TreeRangeMap<UnsignedByte, Pair<UnsignedByte, Integer>>> char2ObjectHashMap) {
        super(char2ObjectHashMap);
    }

    public UBytePairUByteIntRangeMapWritable(TreeRangeMap<UnsignedByte, Pair<UnsignedByte, Integer>> rangeMap) {
        super(rangeMap);
    }

    public UBytePairUByteIntRangeMapWritable(char propertyID, TreeRangeMap<UnsignedByte, Pair<UnsignedByte, Integer>> rangeMap) {
        super(propertyID, rangeMap);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int mapSize = in.readInt(), rangeMapSize;
        while (mapSize > 0) {
            char propertyID = in.readChar();
            TreeRangeMap<UnsignedByte, Pair<UnsignedByte, Integer>> propertyRangeMap = TreeRangeMap.<UnsignedByte, Pair<UnsignedByte, Integer>>create();
            rangeMapSize = in.readInt();
            if (rangeMapSize > 0) {
                UnsignedByte lower, upper;
                Pair<UnsignedByte, Integer> value;
                for (int i = 0; i < rangeMapSize; i++) {
                    lower = new UnsignedByte(in.readByte());
                    upper = new UnsignedByte(in.readByte());
                    value = new MutablePair<>(new UnsignedByte(in.readByte()), in.readInt());
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
            for (Entry<Character, TreeRangeMap<UnsignedByte, Pair<UnsignedByte, Integer>>> property : char2ObjectHashMap.entrySet()) {
                out.writeChar(property.getKey());
                if (property.getValue().asMapOfRanges().size() > 0) {
                    out.writeInt(property.getValue().asMapOfRanges().size());
                    for (Entry<Range<UnsignedByte>, Pair<UnsignedByte, Integer>> entry : property.getValue().asMapOfRanges().entrySet()) {
                        out.writeByte(entry.getKey().lowerEndpoint().byteValue());
                        out.writeByte(entry.getKey().upperEndpoint().byteValue());
                        out.writeByte(entry.getValue().getLeft().byteValue());
                        out.writeInt(entry.getValue().getRight());
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
