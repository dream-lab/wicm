package in.dreamlab.wicm.utils;

import java.util.Iterator;

public class PrimitiveIterable<T> implements Iterable<T>{
    private final PrimitiveIterator<T> iterator;

    public PrimitiveIterable(PrimitiveIterator<T> iterator) {
        this.iterator = iterator;
    }

    @Override
    public Iterator<T> iterator() {
        iterator.reset();
        return iterator;
    }

    public void reset() {
        iterator.reset();
    }

    public void setEnd(int position) {
        iterator.setLastPosition(position);
    }
}
