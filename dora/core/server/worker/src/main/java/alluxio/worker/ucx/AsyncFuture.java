package alluxio.worker.ucx;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Collectively wait for a collection of T results
 * @param <T>
 */
public class AsyncFuture<T> {
  private AtomicInteger mCompletedCount = new AtomicInteger(0);
  private AtomicInteger mTotalExpected = new AtomicInteger(Integer.MAX_VALUE);
  private final CompletableFuture<Boolean> mFuture;

  public AsyncFuture() {
    mFuture = new CompletableFuture<>();
  }

  public AsyncFuture(int totalExpected) {
    mTotalExpected.compareAndSet(Integer.MAX_VALUE, totalExpected);
    mFuture = new CompletableFuture<>();
  }

  public void setTotalExpected(int totalExpected) {
    mTotalExpected.compareAndSet(Integer.MAX_VALUE, totalExpected);
  }

  public void complete(T result) {
    if (mCompletedCount.incrementAndGet() >= mTotalExpected.get()) {
      mFuture.complete(true);
    }
  }

  public void fail(Throwable ex) {
    mFuture.completeExceptionally(ex);
  }

  public boolean get() throws Throwable {
    if (mCompletedCount.incrementAndGet() >= mTotalExpected.get()) {
      mFuture.complete(true);
    }
    return mFuture.get();
  }

  public boolean get(long timeout, TimeUnit unit)
      throws ExecutionException, InterruptedException, TimeoutException {
    if (mCompletedCount.incrementAndGet() >= mTotalExpected.get()) {
      mFuture.complete(true);
    }
    return mFuture.get(timeout, unit);
  }
}
