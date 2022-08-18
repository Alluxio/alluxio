package alluxio.thread;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This thread is able to capture uncaught exceptions from {@code run()}
 * so other classes can check the status of the thread and know why it crashed.
 */
public class AutopsyThread extends Thread {
  /** If the thread meets an uncaught exception, this field will be set. */
  private AtomicReference<Throwable> mThrowable;

  public AutopsyThread() {
    mThrowable = new AtomicReference<>(null);
    setUncaughtExceptionHandler((thread, t) -> {
      mThrowable.set(t);
    });
  }

  public boolean crashed() {
    return mThrowable.get() != null;
  }

  public void setError(Throwable t) {
    mThrowable.set(t);
  }

  @Nullable
  public Throwable getError() {
    return mThrowable.get();
  }
}
