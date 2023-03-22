package alluxio.master.mdsync;

import alluxio.AlluxioURI;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.List;
import java.util.function.Consumer;

class BatchPathWaiter extends BaseTask implements PathWaiter {
  private static final Logger LOG = LoggerFactory.getLogger(BatchPathWaiter.class);
  private static final AlluxioURI EMPTY = new AlluxioURI("");

  final List<PathSequence> mLastCompleted;
  final PathSequence mNoneCompleted;

  BatchPathWaiter(
      TaskInfo info, long startTime, Consumer<Boolean> onComplete,
      Consumer<Throwable> onError) {
    super(info, startTime, onComplete, onError);
    mNoneCompleted = new PathSequence(EMPTY, info.getBasePath());
    mLastCompleted = Lists.newArrayList(mNoneCompleted);
  }

  @VisibleForTesting
  List<PathSequence> getLastCompleted() {
    return mLastCompleted;
  }

  @Override
  public synchronized boolean waitForSync(AlluxioURI path) {
    while (true) {
      if (mIsCompleted != null) {
        return !mIsCompleted.getResult().isPresent();
      }
      PathSequence minCompleted = mLastCompleted.get(0);
      if (minCompleted != mNoneCompleted) {
        if (minCompleted.getStart().compareTo(path) <= 0
            && minCompleted.getEnd().compareTo(path) > 0) {
          return true;
        }
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
    AlluxioURI newRight = null;
    AlluxioURI newLeft = null;
    int i = 0;
    for (; i < mLastCompleted.size(); i++) {
      int rightCmp = mLastCompleted.get(i).getStart().compareTo(completed.getLoaded().getEnd());
      if (rightCmp == 0) {
        newRight = mLastCompleted.get(i).getEnd();
      }
      if (rightCmp >= 0) {
        break;
      }
      int leftCmp = mLastCompleted.get(i).getEnd().compareTo(completed.getLoaded().getStart());
      if (leftCmp == 0) {
        newLeft = mLastCompleted.get(i).getStart();
      }
    }
    if (newRight == null && newLeft == null) {
      mLastCompleted.add(i, completed.getLoaded());
    } else if (newRight != null && newLeft != null) {
      mLastCompleted.set(i, new PathSequence(newLeft, newRight));
      mLastCompleted.remove(i - 1);
    } else if (newLeft != null) {
      mLastCompleted.set(i - 1, new PathSequence(newLeft, completed.getLoaded().getEnd()));
    } else {
      mLastCompleted.set(i, new PathSequence(completed.getLoaded().getStart(), newRight));
    }
    notifyAll();
  }
}
