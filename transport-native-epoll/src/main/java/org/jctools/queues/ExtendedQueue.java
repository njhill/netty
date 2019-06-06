package org.jctools.queues;

import static org.jctools.queues.CircularArrayOffsetCalculator.allocate;
import static org.jctools.queues.LinkedArrayQueueUtil.modifiedCalcElementOffset;
import static org.jctools.util.UnsafeRefArrayAccess.soElement;

import org.jctools.util.RangeUtil;

/**
 * Just a hack to obtain offerAndGetProducerIndex() method which is a trivial
 * modification of the existing offer() method. The other private methods are
 * those used by offer() and copied below verbatim from superclass.
 */
public class ExtendedQueue<E> extends MpscUnboundedArrayQueue<E> {

    private static final Object JUMP;
    private static final int CONTINUE_TO_P_INDEX_CAS = 0;
    private static final int RETRY = 1;
    private static final int QUEUE_FULL = 2;
    private static final int QUEUE_RESIZE = 3;

    static {
        // Fragile hack to get private JUMP sentinel without reflection
        final Object[][] bufBeforeHolder = new Object[1][];
        new MpscUnboundedArrayQueue<Object>(2) {
            {
                bufBeforeHolder[0] = producerBuffer;
                offer("");
                offer("");
            }
        };
        JUMP = bufBeforeHolder[0][1];
        assert JUMP.getClass() == Object.class;
    }

    public ExtendedQueue(int chunkSize) {
        super(chunkSize);
    }

    public long offerAndGetProducerIndex(final E e)
    {
        if (null == e)
        {
            throw new NullPointerException();
        }

        long mask;
        E[] buffer;
        long pIndex;

        while (true)
        {
            long producerLimit = lvProducerLimit();
            pIndex = lvProducerIndex();
            // lower bit is indicative of resize, if we see it we spin until it's cleared
            if ((pIndex & 1) == 1)
            {
                continue;
            }
            // pIndex is even (lower bit is 0) -> actual index is (pIndex >> 1)

            // mask/buffer may get changed by resizing -> only use for array access after successful CAS.
            mask = this.producerMask;
            buffer = this.producerBuffer;
            // a successful CAS ties the ordering, lv(pIndex) - [mask/buffer] -> cas(pIndex)

            // assumption behind this optimization is that queue is almost always empty or near empty
            if (producerLimit <= pIndex)
            {
                int result = offerSlowPath(mask, pIndex, producerLimit);
                switch (result)
                {
                    case CONTINUE_TO_P_INDEX_CAS:
                        break;
                    case RETRY:
                        continue;
                    case QUEUE_FULL:
                        return -1L;
                    case QUEUE_RESIZE:
                        resize(mask, buffer, pIndex, e);
                        return pIndex;
                }
            }

            if (casProducerIndex(pIndex, pIndex + 2))
            {
                break;
            }
        }
        // INDEX visible before ELEMENT
        final long offset = modifiedCalcElementOffset(pIndex, mask);
        soElement(buffer, offset, e); // release element e
        return pIndex;
    }

    private static long nextArrayOffset(long mask)
    {
        return modifiedCalcElementOffset(mask + 2, Long.MAX_VALUE);
    }

    // identical to private superclass version
    private int offerSlowPath(long mask, long pIndex, long producerLimit)
    {
        final long cIndex = lvConsumerIndex();
        long bufferCapacity = getCurrentBufferCapacity(mask);

        if (cIndex + bufferCapacity > pIndex)
        {
            if (!casProducerLimit(producerLimit, cIndex + bufferCapacity))
            {
                // retry from top
                return RETRY;
            }
            else
            {
                // continue to pIndex CAS
                return CONTINUE_TO_P_INDEX_CAS;
            }
        }
        // full and cannot grow
        else if (availableInQueue(pIndex, cIndex) <= 0)
        {
            // offer should return false;
            return QUEUE_FULL;
        }
        // grab index for resize -> set lower bit
        else if (casProducerIndex(pIndex, pIndex + 1))
        {
            // trigger a resize
            return QUEUE_RESIZE;
        }
        else
        {
            // failed resize attempt, retry from top
            return RETRY;
        }
    }

    private void resize(long oldMask, E[] oldBuffer, long pIndex, E e)
    {
        int newBufferLength = getNextBufferSize(oldBuffer);
        final E[] newBuffer = allocate(newBufferLength);

        producerBuffer = newBuffer;
        final int newMask = (newBufferLength - 2) << 1;
        producerMask = newMask;

        final long offsetInOld = modifiedCalcElementOffset(pIndex, oldMask);
        final long offsetInNew = modifiedCalcElementOffset(pIndex, newMask);

        soElement(newBuffer, offsetInNew, e);// element in new array
        soElement(oldBuffer, nextArrayOffset(oldMask), newBuffer);// buffer linked

        // ASSERT code
        final long cIndex = lvConsumerIndex();
        final long availableInQueue = availableInQueue(pIndex, cIndex);
        RangeUtil.checkPositive(availableInQueue, "availableInQueue");

        // Invalidate racing CASs
        // We never set the limit beyond the bounds of a buffer
        soProducerLimit(pIndex + Math.min(newMask, availableInQueue));

        // make resize visible to the other producers
        soProducerIndex(pIndex + 2);

        // INDEX visible before ELEMENT, consistent with consumer expectation

        // make resize visible to consumer
        soElement(oldBuffer, offsetInOld, JUMP);
    }
}
