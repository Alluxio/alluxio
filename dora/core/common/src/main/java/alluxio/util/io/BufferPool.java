package alluxio.util.io;

import alluxio.resource.LockResource;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledDirectByteBuf;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class BufferPool {
    private static final int KB = 1024;
    private static final int MB = KB * KB;

    private final int[] bufSize_ = {128*KB, 1*MB, 4*MB, 8*MB, 16*MB, 32*MB, 64*MB};
    // TOOD mem capped bounded q
    private ConcurrentLinkedQueue<ByteBuf>[] offHeapBuffers_ = new ConcurrentLinkedQueue[bufSize_.length];
    private ConcurrentLinkedQueue<ByteBuf>[] onHeapBuffers_ = new ConcurrentLinkedQueue[bufSize_.length];
    private final AtomicInteger[] onHeapPooledInuseCount_ = new AtomicInteger[bufSize_.length];
    private final AtomicInteger[] offHeapPooledInuseCount_ = new AtomicInteger[bufSize_.length];
    private static final ReentrantLock INSTANCE_LOCK = new ReentrantLock();
    private static AtomicReference<BufferPool> POOL_INSTANCE = new AtomicReference<>();

    public static BufferPool getInstance() {
        if (POOL_INSTANCE.get() == null) {
            try (LockResource lock = new LockResource(INSTANCE_LOCK)) {
                if (POOL_INSTANCE.get() == null) {
                    POOL_INSTANCE.set(new BufferPool());
                }
                return POOL_INSTANCE.get();
            }
        }
        return POOL_INSTANCE.get();
    }

    public BufferPool() {
        for (int i = 0; i < bufSize_.length; i++) {
            offHeapBuffers_[i] = new ConcurrentLinkedQueue<ByteBuf>();
            onHeapBuffers_[i] = new ConcurrentLinkedQueue<ByteBuf>();
            onHeapPooledInuseCount_[i] = new AtomicInteger(0);
            offHeapPooledInuseCount_[i] = new AtomicInteger(0);
        }
    }

    public ByteBuf getABuffer(int size, boolean isDirect) {
        ByteBuf retBuf = null;
        int idx = Arrays.binarySearch(bufSize_, size);
        if (idx >= bufSize_.length) {
            if (isDirect) {
                retBuf = Unpooled.directBuffer(size, size);
            } else {
                retBuf = Unpooled.buffer(size, size);
            }
            return retBuf;
        }
        if (idx < 0)
            idx = -idx-1;
        if (isDirect) {
            retBuf = offHeapBuffers_[idx].poll();
        } else {
            retBuf = onHeapBuffers_[idx].poll();
        }
        if (retBuf != null)
            return retBuf;
        if (isDirect) {
            retBuf = Unpooled.directBuffer(bufSize_[idx], bufSize_[idx]);
            offHeapPooledInuseCount_[idx].incrementAndGet();
        } else {
            retBuf = Unpooled.buffer(bufSize_[idx], bufSize_[idx]);
            onHeapPooledInuseCount_[idx].incrementAndGet();
        }
        retBuf.clear();
        return retBuf;
    }

    public void returnBuffer(ByteBuf buffer) {
        Preconditions.checkState(buffer.refCnt() == 1,
            "buffer not from pool but refcnt non-zero, leaking situation.");
        buffer.clear();
        boolean isDirect = buffer.isDirect();
        int idx = Arrays.binarySearch(bufSize_, buffer.capacity());
        boolean fromPool = idx >= 0 && idx < bufSize_.length;
        if (!fromPool) {
            buffer.release();
        } else {
            if (isDirect) {
                if (thresholdViolated(false)) {
                    buffer.release();
                    return;
                }
                offHeapBuffers_[idx].offer(buffer);
                offHeapPooledInuseCount_[idx].decrementAndGet();
            } else {
                if (thresholdViolated(true)) {
                    buffer.release();
                    return;
                }
                onHeapBuffers_[idx].offer(buffer);
                onHeapPooledInuseCount_[idx].decrementAndGet();
            }
        }
    }

    public static final long heapMax = (long)(Runtime.getRuntime().maxMemory() * 0.10);
    public static final long offheapMax = (long)(Runtime.getRuntime().maxMemory() * 0.10);

    public boolean thresholdViolated(boolean checkOnHeap) {
        if (checkOnHeap) {
            long curOnHeap = 0;
            for (int i=0; i<bufSize_.length; i++) {
                curOnHeap += onHeapBuffers_[i].size() * bufSize_[i];
            }
            if (curOnHeap >= heapMax) {
                return true;
            }
            return false;
        } else {
            long curOffHeap = 0;
            for (int i = 0; i < bufSize_.length; i++) {
                curOffHeap += offHeapBuffers_[i].size() * bufSize_[i];
            }
            if (curOffHeap > offheapMax) {
                return true;
            }
            return false;
        }
    }

    public String printBufferStats() {
        StringBuilder sb = new StringBuilder();
        sb.append("----On Heap----");
        String formatStr = "%s\t\t%s\t\t%s\t\t%s\n";
        sb.append(String.format(formatStr, "Size", "InUseCount", "AvailableCount"));
        long totalPooledOnHeap = 0;
        for (int i=0; i<bufSize_.length; i++) {
            int onHeapPooledInuseCount = onHeapPooledInuseCount_[i].get();
            int onHeapPooledAvailableCount = onHeapBuffers_[i].size();
            sb.append(String.format(formatStr,
                bufSize_[i],
                onHeapPooledInuseCount,
                onHeapPooledAvailableCount));
            totalPooledOnHeap += bufSize_[i] * (onHeapPooledInuseCount + onHeapPooledAvailableCount);
        }
        sb.append(String.format("Total Pooled on heap:%s\n" + totalPooledOnHeap));
        sb.append("----Off Heap----");
        sb.append(String.format(formatStr, "Size", "InUseCount", "AvailableCount"));
        long totalPooledOffHeap = 0;
        for (int i=0; i<bufSize_.length; i++) {
            int offHeapPooledInuseCount = onHeapPooledInuseCount_[i].get();
            int offHeapPooledAvailableCount = onHeapBuffers_[i].size();
            sb.append(String.format(formatStr,
                bufSize_[i],
                offHeapPooledInuseCount_[i].get(),
                offHeapBuffers_[i].size()));
            totalPooledOffHeap += bufSize_[i] * (offHeapPooledInuseCount + offHeapPooledAvailableCount);
        }
        sb.append(String.format("Total Pooled off heap:%s\n" + totalPooledOffHeap));
        return sb.toString();
    }

    public static void main(String[] args)
    {
        try {
            BufferPool bufferPool = new BufferPool();
            int idx = Arrays.binarySearch(bufferPool.bufSize_, 16*KB + 1);
            System.out.println(idx);
            /*
            Scanner scanner = new Scanner(System.in);
            List<ByteBuf> bufferList = new ArrayList<>();
            while (true) {
                String nextCmd = scanner.nextLine();
                switch (nextCmd.toLowerCase()) {
                    case "alloc":
//                        ByteBuffer nioBuffer = ByteBuffer.allocateDirect((int)(64*MB));
                        for (int j=0;j<64;j++) {
                            ByteBuf buffer = Unpooled.directBuffer((int) (4 * MB));
                            System.out.println("addr:" + buffer.memoryAddress());
                            for (int i = 0; i < 4 * MB; i += Long.BYTES)
                                buffer.writeLong(0L);
                            bufferList.add(buffer);
                        }
                        break;
                    case "clean":
                        for (ByteBuf buffer : bufferList) {
                            ByteBuffer internalBuf = buffer.nioBuffer();
                            BufferUtils.cleanDirectBuffer(internalBuf);
                            System.out.println("=> cleaned.");
                        }
                        break;
                    case "release":
                        for (ByteBuf buffer : bufferList) {
                            if (buffer != null) {
                                boolean released = buffer.release();
                                buffer = null;
                                System.out.println("=> released : " + released);
                            }
                        }
                        bufferList.clear();
                        break;
                    case "gc":
                        System.out.printf("Memory used before %,d MB%n", mbUsed());
                        System.gc();
                        System.out.printf("Memory used after %,d MB%n", mbUsed());
                        break;
                    case "exit":
                        return;
                    default:
                        System.out.println("Unknown.");
                        break;
                }
            }
            */
        } catch (Throwable th)
        {
            System.out.println(th.getMessage());
        }
    }

    private static long mbUsed() {
        return (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())/1024/1024;
    }
}
