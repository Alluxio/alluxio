package alluxio.master.mdsync;

import alluxio.AlluxioURI;
import alluxio.conf.path.TrieNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

class DirectoryPathWaiter extends BaseTask implements PathWaiter {
  private static final Logger LOG = LoggerFactory.getLogger(DirectoryPathWaiter.class);

  private final TrieNode<AlluxioURI> mCompletedDirs = new TrieNode<>();

  DirectoryPathWaiter(
      TaskInfo info, long startTime, Consumer<Boolean> onComplete,
      Consumer<Throwable> onError) {
    super(info, startTime, onComplete, onError);
  }

  @Override
  public synchronized boolean waitForSync(AlluxioURI path) {
    while (true) {
      if (mIsCompleted != null) {
        return !mIsCompleted.getResult().isPresent();
      }
      boolean completed = mCompletedDirs.getClosestTerminal(path.getPath())
          .map(result -> {
            if (result.getValue().equals(path)) {
              return true;
            }
            AlluxioURI parent = path.getParent();
            return parent != null && parent.equals(result.getValue());
          }).orElse(false);
      if (completed) {
        return true;
      }
      try {
        wait();
      } catch (InterruptedException e) {
        LOG.debug("Interrupted while waiting for synced path {}", path);
        return false;
      }
    }
  }

  @Override
  public synchronized void nextCompleted(SyncProcessResult completed) {
    if (!completed.isTruncated()) {
      mCompletedDirs.insert(completed.getBaseLoadPath().getPath())
          .setValue(completed.getBaseLoadPath());
      notifyAll();
    }
  }
}
