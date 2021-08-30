package in.dreamlab.wicm.utils;

import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.UnsafeByteArrayInputStream;
import org.apache.giraph.utils.UnsafeByteArrayOutputStream;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.lang.reflect.Array;

public class LocalWritableMessageBuffer<T extends Writable> {
    private Class<T> type;
    private UnsafeByteArrayOutputStream dOut;
    private UnsafeByteArrayInputStream dIn;

    private int pointer = 0;
    private T[] messageBlock;
    private PrimitiveIterable<T> localMessages;

    public LocalWritableMessageBuffer(int maxBufferSize, Class<T> type) {
        this.type = type;
        dOut = new UnsafeByteArrayOutputStream();
        dIn = new UnsafeByteArrayInputStream(dOut.getByteArray(), 0, dOut.getPos());

        messageBlock = (T[]) Array.newInstance(type, maxBufferSize);
        for(int i=0; i<maxBufferSize; i++)
            messageBlock[i] = ReflectionUtils.newInstance(type);
        localMessages = new PrimitiveIterable<>(new PrimitiveIterator<>(messageBlock));
        pointer = 0;
    }

    public void reset() {
        pointer = 0;
    }

    public boolean isEmpty() {
        return pointer == 0;
    }

    public int filledBufferSize() {
        return pointer;
    }

    public boolean addMessage(T message) throws IOException {
        if(pointer < messageBlock.length) {
            dOut.reset();
            message.write(dOut);
            dIn.setBuffer(dOut.getByteArray(), 0, dOut.getPos());
            messageBlock[pointer++].readFields(dIn);
            return true;
        }
        return false;
    }

    public Iterable<T> getIterable() {
        localMessages.setEnd(pointer);
        return localMessages;
    }
}