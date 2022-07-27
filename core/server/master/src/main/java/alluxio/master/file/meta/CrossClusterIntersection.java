package alluxio.master.file.meta;

import alluxio.collections.ConcurrentHashSet;
import alluxio.conf.path.TrieNode;
import alluxio.resource.LockResource;

import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

/**
 * Track mappings of ufs mounts to alluxio clusters.
 */
public class CrossClusterIntersection {

  private final ReentrantReadWriteLock mLock = new ReentrantReadWriteLock();
  private final TrieNode<Set<String>> mMappings
      = new TrieNode<>();

  /**
   * Add a mapping.
   * @param path
   * @param clusterID
   */
  public void addMapping(String path, String clusterID) {
    try (LockResource ignored = new LockResource(mLock.writeLock())) {
      TrieNode<Set<String>> node = mMappings.insert(path);
      Set<String> items = node.getValue();
      if (items == null) {
        items = new ConcurrentHashSet<>();
        node.setValue(items);
      }
      items.add(clusterID);
    }
  }

  /**
   * Remove a mapping.
   * @param path
   * @param clusterID
   */
  public void removeMapping(String path, String clusterID) {
    try (LockResource ignored = new LockResource(mLock.writeLock())) {
      mMappings.deleteIf(path, (node) -> {
        Set<String> set = node.getValue();
        if (set != null) {
          set.remove(clusterID);
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
  public Stream<String> getClusters(String path) {
    try (LockResource ignored = new LockResource(mLock.readLock())) {
      List<TrieNode<Set<String>>> components = mMappings.search(path);
      return components.stream().flatMap(nxt -> {
        Set<String> value = nxt.getValue();
        if (value != null) {
          return value.stream();
        }
        return Stream.empty();
      }).distinct();
    }
  }
}
