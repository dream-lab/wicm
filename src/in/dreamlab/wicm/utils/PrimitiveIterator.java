package in.dreamlab.wicm.utils;

import java.util.Iterator;

public class PrimitiveIterator<T> implements Iterator<T> {
    private int position;
    private int lastPosition;
    private final T[] array;

    public PrimitiveIterator(T[] array) {
        this.array = array;
        lastPosition = array.length;
        position = 0;
    }

    @Override
    public boolean hasNext() {
        return position < lastPosition;
    }

    @Override
    public T next() {
        return array[position++];
    }

    public void reset() {
        position = 0;
    }

    public void setLastPosition(int position) {
        lastPosition = Integer.min(position, array.length);
    }
}
