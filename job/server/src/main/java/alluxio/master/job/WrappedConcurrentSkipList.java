package alluxio.master.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

public class WrappedConcurrentSkipList<K, V> extends ConcurrentSkipListMap<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(WrappedConcurrentSkipList.class);

  @Override
  public boolean isEmpty() {
    synchronized (this) {
      logSizeMsg("isEmpty", true);
      boolean e = super.isEmpty();
      logSizeMsg("isEmpty", false);
      return e;
    }
  }

  @Override
  public V remove(Object k) {
    synchronized (this) {
      logSizeMsg("remove", true);
      V e = super.remove(k);
      logSizeMsg("remove", false);
      return e;
    }
  }

  @Override
  public Map.Entry<K, V> firstEntry() {
    synchronized (this) {
      logSizeMsg("firstEntry", true);
      Map.Entry<K, V> e = super.firstEntry();
      logSizeMsg("firstEntry", false);
      return e;
    }
  }

  @Override
  public V put(K key, V value) {
    synchronized (this) {
      logSizeMsg("put", true);
      V v = super.put(key, value);
      logSizeMsg("put", false);
      return v;
    }
  }


  private void logSizeMsg(String methodName, boolean before) {
    LOG.info("size {} {}: {}", before ? "before" : "after", methodName, size());
  }
}
