package alluxio.shuttle.executor;

import alluxio.shuttle.Constants;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Use a bounded blocking queue to store tasks for one thread
 */
public class DefaultThreadExecutor extends SingleThreadExecutor{
    private final BlockingQueue<Runnable> queueRefer;

    public DefaultThreadExecutor(String name, int queueSize, int waitMs) {
        super(name, queueSize, waitMs);
        queueRefer = (BlockingQueue<Runnable>) taskQueue;
    }


    public DefaultThreadExecutor(String name) {
        this(name, Constants.DEFAULT_MSG_QUEUE_SIZE, 0);
    }

    @Override
    protected Queue<Runnable> createTaskQueue(int queueSize) {
        return new ArrayBlockingQueue<>(queueSize);
    }

    @Override
    protected Runnable pollTask() throws InterruptedException {
       return queueRefer.poll(Constants.PAUSE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    @Override
    protected boolean offerTask(Runnable task) throws InterruptedException {
        if (waitMs <= 0) {
            queueRefer.put(task);
            return true;
        } else {
            return queueRefer.offer(task, waitMs, TimeUnit.MILLISECONDS);
        }
    }
}
