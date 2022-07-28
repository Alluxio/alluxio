package alluxio.master.file.meta;

import alluxio.conf.path.TrieNode;
import alluxio.resource.LockResource;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Track mappings of ufs mounts to set of values.
 * @param <V> the type of the values stored
 */
public class CrossClusterIntersection<V> {

  private final ReentrantReadWriteLock mLock = new ReentrantReadWriteLock();
  private final TrieNode<Map<String, V>> mMappings
      = new TrieNode<>();

  /**
   * Add a mapping.
   * @param clusterId the cluster id
   * @param path
   * @param value the value
   */
  public void addMapping(String clusterId, String path, V value) {
    try (LockResource ignored = new LockResource(mLock.writeLock())) {
      TrieNode<Map<String, V>> node = mMappings.insert(path);
      Map<String, V> items = node.getValue();
      if (items == null) {
        items = new ConcurrentHashMap<>();
        node.setValue(items);
      }
      items.put(clusterId, value);
    }
  }

  /**
   * Remove a mapping.
   * @param clusterId the cluster id
   * @param path the path to remove
   * @param removeIf remove if this function resolves to true, or if this is null
   */
  public void removeMapping(
      String clusterId, String path, @Nullable Function<V, Boolean> removeIf) {
    try (LockResource ignored = new LockResource(mLock.writeLock())) {
      mMappings.deleteIf(path, (node) -> {
        Map<String, V> set = node.getValue();
        if (set != null) {
          if (removeIf != null) {
            V value = set.get(clusterId);
            if (value != null && removeIf.apply(value)) {
              set.remove(clusterId);
            }
          } else {
            set.remove(clusterId);
          }
        }
        return set == null || set.isEmpty();
      });
    }
  }

  /**
   * Get the clusters who intersect the given path.
   * @param path
   * @return empty
   */
  public Stream<V> getClusters(String path) {
    try (LockResource ignored = new LockResource(mLock.readLock())) {
      List<TrieNode<Map<String, V>>> components = mMappings.search(path);
      return components.stream().flatMap(nxt -> {
        Map<String, V> value = nxt.getValue();
        if (value != null) {
          return value.values().stream();
        }
        return Stream.empty();
      }).distinct();
    }
  }
}
