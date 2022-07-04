package alluxio.shuttle.executor;

import alluxio.exception.ShuttleRpcExecutorException;
import alluxio.exception.ShuttleRpcRuntimeException;
import alluxio.shuttle.Constants;
import org.apache.commons.codec.digest.MurmurHash3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Thread pool model as grouping tasks by specified thread id.
 * The read / write tasks of same file will put the queue of one thread, et.
 * Thread id depends on the hash value of label.
 */
public abstract class GroupExecutor extends AbstractExecutorService {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    protected final List<SingleThreadExecutor> threadGroup;
    protected final int threadNum;
    protected final int queueSize;
    protected final int waitMs;
    protected volatile boolean stopped = false;

    public GroupExecutor(
            int threadNum,
            int queueSize,
            int waitMs) {
        this.threadNum = threadNum > 0 ? threadNum : Constants.DEFAULT_GROUP_THREAD_NUM;
        this.queueSize = queueSize > 0 ? queueSize : Constants.DEFAULT_MSG_QUEUE_SIZE;
        this.waitMs = waitMs;
        this.threadGroup = createThreadGroup();

        LOG.info("{} create success, treadNum: {}, queueSize {}, waitMs {}",
                getClass().getSimpleName(), this.threadNum, this.queueSize, this.waitMs);
    }

    public GroupExecutor() {
        this(0, 0, 0);
    }

    public GroupExecutor(int threadNum) {
        this(threadNum, 0, 0);
    }

    protected abstract List<SingleThreadExecutor> createThreadGroup();

    protected abstract SingleThreadExecutor getThread(long value);

    protected Iterator<SingleThreadExecutor> allThreadsIterator() {
        return threadGroup.iterator();
    }

    public Thread getJavaThread(long value) {
        return getThread(value).thread;
    }

    /**
     * Put the runnable task into the queue of specified thread by threadId
     * @param threadId specified thread
     * @param command task
     * @throws ShuttleRpcExecutorException
     */
    public void execute(long threadId, Runnable command) throws ShuttleRpcExecutorException {
        checkStop();
        getThread(threadId).execute(command);
    }

    @Override
    public void execute(@Nonnull Runnable runnable) {
        throw new ShuttleRpcRuntimeException("Execute thread index is required in GroupExecutor!");
    }

    /**
     * Get thread id
     * @param value
     * @return
     */
    public long getIndex(long value) {
        return Math.abs(MurmurHash3.hash32(value) % threadNum);
    }

    public String threadName(int i) {
        return this.getClass().getSimpleName() + "-" + i;
    }

    public String threadName(long i) {
        return this.getClass().getSimpleName() + "-" + i;
    }

    public String threadName(String name) {
        return this.getClass().getSimpleName() + "-" + name;
    }

    public int getPendingTask(long value) {
        return getThread(value).getPendingTask();
    }

    protected void checkStop() throws ShuttleRpcExecutorException {
        if (stopped) {
            throw new ShuttleRpcExecutorException("The thread pool has been stopped");
        }
    }

    @Override
    public void shutdown() {
        shutdown(0);
    }

    public synchronized void shutdown(long timeoutMs) {
        if (stopped) {
            return;
        }

        for (Iterator<SingleThreadExecutor> it = allThreadsIterator(); it.hasNext(); ) {
            SingleThreadExecutor shareThread = it.next();
            shareThread.shutdown(timeoutMs);
        }
        stopped = true;
    }

    @Override
    public List<Runnable> shutdownNow() {
        shutdown();
        return Collections.emptyList();
    }

    @Override
    public boolean isTerminated() {
        return stopped;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        loop:
        for (Iterator<SingleThreadExecutor> it = allThreadsIterator(); it.hasNext(); ) {
            SingleThreadExecutor thread = it.next();
            for (;;) {
                long timeLeft = deadline - System.nanoTime();
                if (timeLeft <= 0) {
                    break loop;
                }
                if (thread.awaitTermination(timeLeft, TimeUnit.NANOSECONDS)) {
                    break;
                }
            }
        }

        return isTerminated();
    }

    @Override
    public boolean isShutdown() {
        for (Iterator<SingleThreadExecutor> it = allThreadsIterator(); it.hasNext(); ) {
            SingleThreadExecutor thread = it.next();
            if (!thread.isShutdown()) {
                return false;
            }
        }

        return true;
    }

    public int getThreadNum() {
        return threadNum;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public int getWaitMs() {
        return waitMs;
    }
}
