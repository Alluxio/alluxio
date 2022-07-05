package alluxio.shuttle.executor;

import alluxio.exception.ShuttleRpcRuntimeException;
import alluxio.shuttle.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Single thread and specified task queue.
 * Task queue is blocking queue, stores task by FIFO sequence.
 * Thread used to process the task from the queue.
 */
public abstract class SingleThreadExecutor extends AbstractExecutorService {
    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    protected final String name;
    protected final int queueSize;
    protected final int waitMs;
    protected final Thread thread;
    protected final Queue<Runnable> taskQueue;

    protected final AtomicInteger ctl = new AtomicInteger(Constants.ThreadStatus.INIT.getValue());

    protected Callable<Boolean> END_TASK = () -> {
        advanceRunState(Constants.ThreadStatus.STOP.getValue());
        return true;
    };

    public SingleThreadExecutor(String name, int queueSize, int waitMs) {
        this.name = name;
        this.queueSize = queueSize;
        this.waitMs = waitMs;
        this.taskQueue = createTaskQueue(queueSize);
        thread = new Thread(this::run);
        thread.setName(name);
        thread.setDaemon(true);
        thread.start();
        advanceRunState(Constants.ThreadStatus.RUNNING.getValue());
    }

    public SingleThreadExecutor(String name) {
        this(name, Constants.DEFAULT_MSG_QUEUE_SIZE, 0);
    }

    protected abstract Queue<Runnable> createTaskQueue(int queueSize);

    protected abstract Runnable pollTask() throws InterruptedException;

    protected abstract boolean offerTask(Runnable task) throws InterruptedException;

    @Override
    public void execute(@Nonnull Runnable task) {
        if (isShutdown()) {
            throw new ShuttleRpcRuntimeException("The thread has been shutdown, cannot add tasks");
        }

        try {
            boolean bool = offerTask(task);
            if (!bool) {
                String msg = String.format("thread %s queue is full(queueSize=%s)", name, queueSize);
                throw new ShuttleRpcRuntimeException(msg);
            }
        } catch (InterruptedException e) {
            LOG.info("thread {} quit", name);
        }
    }

    public void run() {
        Runnable task;
        while (hasRunnableTask()) {
            try {
                while ((task = pollTask()) != null) {
                    task.run();
                }
            } catch (Throwable e) {
                LOG.error("task failed", e);
            }
        }
    }

    public int getPendingTask() {
        return taskQueue.size();
    }

    /**
     * SHUTDOWN is a transitional state, new tasks cannot be added,
     * but tasks in the queue can continue to be executed
     */
    protected boolean hasRunnableTask() {
        return ctl.get() <= Constants.ThreadStatus.SHUTDOWN.getValue();
    }

    /**
     * Must be in running state to add new tasks
     */
    protected boolean canAddTask() {
        return ctl.get() <= Constants.ThreadStatus.RUNNING.getValue();
    }

    public boolean stopped() {
        return ctl.get() == Constants.ThreadStatus.STOP.getValue();
    }

    private static boolean runStateAtLeast(int c, int s) {
        return c >= s;
    }

    public void advanceRunState(int targetState) {
        for (;;) {
            int c = ctl.get();
            if (runStateAtLeast(c, targetState) || ctl.compareAndSet(c, targetState)) {
                break;
            }
        }
    }

    public void shutdown(long timeoutMs) {
        if (isShutdown()) {
            LOG.warn("The thread has been shutdown");
            return;
        }

        advanceRunState(Constants.ThreadStatus.SHUTDOWN.getValue());

        long wait = 0;
        while(taskQueue.size() != 0) {
            if (timeoutMs > 0 && wait >= waitMs) {
                break;
            }
            long start = System.currentTimeMillis();
            try {
                Thread.sleep(Constants.PAUSE_TIMEOUT_MS);
            } catch (InterruptedException e) {
                LOG.warn("thread shutdown", e);
            }
            wait += System.currentTimeMillis() - start;
        }

        advanceRunState(Constants.ThreadStatus.STOP.getValue());

        if (thread != Thread.currentThread()) {
            try {
                thread.join(timeoutMs);
            } catch (InterruptedException e) {
                // pass
            }
        }

        if (thread.isAlive()) {
            thread.interrupt();
        }
    }

    @Override
    public void shutdown() {
        shutdown(0);
    }

    public Future<Boolean> sendStop() {
        return submit(END_TASK);
    }

    @Override
    public List<Runnable> shutdownNow() {
        shutdown();
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
        return ctl.get() >= Constants.ThreadStatus.SHUTDOWN.getValue();
    }

    @Override
    public boolean isTerminated() {
        return ctl.get() >= Constants.ThreadStatus.STOP.getValue();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        thread.join(unit.toMillis(timeout));
        return isTerminated();
    }

    public String getName() {
        return name;
    }
}
