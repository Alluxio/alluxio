package alluxio.util;

import alluxio.util.io.FileUtils;

import com.google.common.base.Preconditions;
import com.sun.management.HotSpotDiagnosticMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.NoSuchFileException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.MBeanServer;

/**
 * Dumps heaps on a separate thread on a fixes interval.
 */
public class JvmHeapDumper extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(JvmHeapDumper.class);
  private static final String HOTSPOT_BEAN_NAME = "com.sun.management:type=HotSpotDiagnostic";

  private final MBeanServer mServer = ManagementFactory.getPlatformMBeanServer();
  HotSpotDiagnosticMXBean mXBean;
  private final long mInterval;
  private final String mDirPath;
  private final String mDumpPrefix;
  private AtomicBoolean mStop = new AtomicBoolean(false);

  /**
   * Heap dumper.
   * @param intervalMs interval to make dumps on
   * @param dirPath path to store dumps
   * @param prefix prefix for dumps
   */
  public JvmHeapDumper(long intervalMs, String dirPath, String prefix) {
    super("Thread-Heap-Dumper");
    Preconditions.checkArgument(intervalMs > 0);
    mDumpPrefix = Preconditions.checkNotNull(prefix);
    Preconditions.checkNotNull(dirPath);
    mInterval = intervalMs;
    try {
      mXBean = ManagementFactory.newPlatformMXBeanProxy(mServer,
          HOTSPOT_BEAN_NAME, HotSpotDiagnosticMXBean.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    File f = new File(String.format("%s/%s", System.getProperty("user.dir"), dirPath));
    try {
      FileUtils.deletePathRecursively(f.getAbsolutePath());
    } catch (NoSuchFileException e) {
      // It's ok if the path doesn't exist already
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (!f.mkdirs()) {
      throw new RuntimeException("Couldn't create dirs " + f.getAbsolutePath());
    }
    mDirPath = f.getAbsolutePath();
  }

  public void stopDumps() throws InterruptedException {
    mStop.set(true);
    join();
  }

  @Override
  public void run() {
    int i = 0;
    while (!mStop.get()) {
      try {
        dumpHeap(i);
      } catch (IOException e) {
        // ok
        System.out.println(e.toString());
        LOG.warn(e.toString());
      }
      try {
        Thread.sleep(mInterval);
      } catch (InterruptedException e) {
        // ok
        System.out.println(e.toString());
        LOG.warn(e.toString());
      }
      i++;
    }
  }

  private void dumpHeap(int num) throws IOException {
    String dumpLocation = String.format("%s/%s-%d.hprof", mDirPath, mDumpPrefix, num);
    mXBean.dumpHeap(dumpLocation, true);
  }
}

