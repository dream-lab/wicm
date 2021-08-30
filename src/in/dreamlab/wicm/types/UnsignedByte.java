package in.dreamlab.wicm.types;

public class UnsignedByte extends Number implements Comparable<UnsignedByte> {
    public static final int MIN_VALUE = 0;
    public static final int MAX_VALUE = 255;
    private final byte value;

    public UnsignedByte(byte var1) {
        this.value = ((byte) Byte.toUnsignedInt(var1));
    }

    public UnsignedByte(int var1) {
        this.value = ((byte) var1);
    }

    public byte byteValue() {
        return this.value;
    }

    public short shortValue() {
        return (short)Byte.toUnsignedInt(this.value);
    }

    public int intValue() {
        return Byte.toUnsignedInt(this.value);
    }

    public long longValue() {
        return (long)Byte.toUnsignedInt(this.value);
    }

    public float floatValue() {
        return (float)Byte.toUnsignedInt(this.value);
    }

    public double doubleValue() {
        return (double)Byte.toUnsignedInt(this.value);
    }

    public String toString() {
        return Integer.toString(Byte.toUnsignedInt(this.value));
    }

    public int hashCode() {
        return hashCode(this.value);
    }

    public static int hashCode(byte var0) {
        return var0;
    }

    public boolean equals(Object var1) {
        if (var1 instanceof UnsignedByte) {
            return this.value == ((UnsignedByte) var1).value;
        } else {
            return false;
        }
    }

    public int compareTo(UnsignedByte var1) {
        return Integer.compare(intValue(), var1.intValue());
    }
}