/**
 * <pre>
 * Copy from <href>https://github.com/google/guava/blob/master/guava/src/com/google/common/collect/Lists.java</href> 
 * It's Apache License.
 * </pre>
 */
package com.youzan.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.RoundingMode;
import java.util.AbstractList;
import java.util.List;
import java.util.RandomAccess;

/**
 * @author <a href="mailto:my_email@email.exmaple.com">zhaoxi (linzuxiong)</a>
 *
 * 
 */
public class Lists {
    private static final Logger logger = LoggerFactory.getLogger(Lists.class);

    public static <T> List<List<T>> partition(List<T> list, int size) {
        if (list == null || size <= 0) {
            throw new IllegalArgumentException("Your input is blank!");
        }
        return (list instanceof RandomAccess) ? new RandomAccessPartition<T>(list, size) : new Partition<T>(list, size);
    }

    private static class Partition<T> extends AbstractList<List<T>> {
        final List<T> list;
        final int size;

        Partition(List<T> list, int size) {
            this.list = list;
            this.size = size;
        }

        @Override
        public List<T> get(int index) {
            if (index < 0 || index >= size()) {
                throw new IndexOutOfBoundsException(String.format("Size: %s, Index: %s", size(), index));
            }
            int start = index * size;
            int end = Math.min(start + size, list.size());
            return list.subList(start, end);
        }

        @Override
        public int size() {
            return divideInt(list.size(), size, RoundingMode.CEILING);
        }

        @Override
        public boolean isEmpty() {
            return list.isEmpty();
        }
    }

    private static class RandomAccessPartition<T> extends Partition<T> implements RandomAccess {
        RandomAccessPartition(List<T> list, int size) {
            super(list, size);
        }
    }

    /**
     * Returns the base-2 logarithm of {@code x}, rounded according to the
     * specified rounding mode.
     *
     * @param p
     *            the size
     * @param q
     *            the small batch size
     * @param mode
     *            RoundingMode
     * 
     * @return batches
     * @throws IllegalArgumentException
     *             if {@code x <= 0}
     * @throws ArithmeticException
     *             if {@code mode} is {@link RoundingMode#UNNECESSARY} and
     *             {@code x} is not a power of two
     */
    public static int divideInt(int p, int q, RoundingMode mode) {
        if (q == 0) {
            throw new ArithmeticException("/ by zero"); // for GWT
        }
        int div = p / q;
        int rem = p - q * div; // equal to p % q

        if (rem == 0) {
            return div;
        }
        int signum = 1 | ((p ^ q) >> (Integer.SIZE - 1));
        boolean increment;
        switch (mode) {
            case DOWN:
                increment = false;
                break;
            case UP:
                increment = true;
                break;
            case CEILING:
                increment = signum > 0;
                break;
            case FLOOR:
                increment = signum < 0;
                break;
            case HALF_EVEN:
            case HALF_DOWN:
            default:
                throw new AssertionError();
        }
        return increment ? div + signum : div;
    }
}
