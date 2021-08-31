package in.dreamlab.wicm.types;

public class VariableInteger extends Number implements Comparable<VariableInteger> {
    private final byte[] value;

    // SOURCE: https://golb.hplar.ch/2019/06/variable-length-int-java.html
    public static byte[] encodeUInt32(int inputValue) {
        int value = inputValue;
        byte[] buffer = new byte[5];
        int position = 0;

        while (true) {
            // ~0x7F = 0xffffff80
            if ((value & 0b11111111111111111111111110000000) == 0) {
                buffer[position++] = (byte) value;
                break;
            }

            buffer[position++] = (byte) ((value & 0b1111111) | 0b10000000);
            value >>>= 7;
        }

        byte[] dest = new byte[position];
        System.arraycopy(buffer, 0, dest, 0, position);
        return dest;
    }

    // SOURCE: https://golb.hplar.ch/2019/06/variable-length-int-java.html
    public static int decodeUInt32(byte[] input) {
        int result = 0;
        int shift = 0;
        for (int ix = 0; ix < input.length; ix++) {
            byte b = input[ix];
            result |= (b & 0b1111111) << shift;
            shift += 7;
            if ((b & 0b10000000) == 0) {
                return result;
            }
        }
        return result;
    }

    public VariableInteger(int var1) {
        this.value = encodeUInt32(var1);
    }

    public byte byteValue() {
        return ((byte) decodeUInt32(value));
    }

    public int intValue() {
        return decodeUInt32(value);
    }

    public long longValue() {
        return decodeUInt32(value);
    }

    @Override
    public float floatValue() {
        return ((float) decodeUInt32(value));
    }

    @Override
    public double doubleValue() {
        return decodeUInt32(value);
    }

    public String toString() {
        return Integer.toString(decodeUInt32(value));
    }

    public int hashCode() {
        return Integer.hashCode(decodeUInt32(value));
    }

    public static int hashCode(byte var0) {
        return var0;
    }

    public boolean equals(Object var1) {
        if (var1 instanceof VariableInteger) {
            return decodeUInt32(value) == ((VariableInteger) var1).intValue();
        } else {
            return false;
        }
    }

    @Override
    public int compareTo(VariableInteger o) {
        return Integer.compare(decodeUInt32(value), ((VariableInteger) o).intValue());
    }
}