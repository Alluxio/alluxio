package alluxio.worker.ucx;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Stage {
  private final ThreadPoolExecutor mInternalThreadPool = null;

  public Stage(String stageName,
                     int corePoolSize,
                     int maximumPoolSize,
                     ThreadFactory threadFactory,
                     BlockingQueue<Runnable> workQueue,
                     RejectedExecutionHandler handler) {


  }

}
