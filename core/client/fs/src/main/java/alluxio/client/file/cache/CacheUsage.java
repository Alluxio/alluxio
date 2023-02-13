/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file.cache;

import alluxio.client.quota.CacheScope;

import com.google.common.base.MoreObjects;

import java.util.Objects;
import java.util.Optional;

/**
 * Cache usage.
 */
public interface CacheUsage extends CacheUsageView {
  /**
   * Creates an immutable snapshot of the current cache usage.
   *
   * @return the snapshot
   */
  default CacheUsageView snapshot() {
    return new ImmutableCacheUsageView(used(), available(), capacity());
  }

  /**
   * Gets a finer-grained view of cache usage.
   * <br> Example of getting cache usage of a particular file:
   * <br>
   * <code><pre>
   * cacheManager.getUsage()
   *     .flatMap(usage -> usage.partitionedBy(PartitionDescriptor.file(fileId))
   *     .map(CacheUsage::used)
   *     .orElse(0)
   * </pre></code>
   *
   * @param partition how to partition the cache
   * @return partitioned view of cache usage, none if this usage object does not support
   *         partitioning, or usage info for the specified partition is not found
   */
  Optional<CacheUsage> partitionedBy(PartitionDescriptor<?> partition);

  /**
   * Partition descriptor.
   *
   * @param <T> type of the identifier
   */
  interface PartitionDescriptor<T> {
    /**
     * Gets an identifier of the partition.
     *
     * @return identifier
     */
    T getIdentifier();

    /**
     * Creates a partition for a specific file.
     *
     * @param fileId file ID
     * @return the partition
     */
    static FilePartition file(String fileId) {
      return new FilePartition(fileId);
    }

    /**
     * Creates a partition for a directory.
     *
     * @param index dir index
     * @return the partition
     */
    static DirPartition dir(int index) {
      return new DirPartition(index);
    }

    /**
     * Creates a partition of a cache scope.
     *
     * @param scope the cache scope
     * @return the partition
     */
    static ScopePartition scope(CacheScope scope) {
      return new ScopePartition(scope);
    }
  }

  /**
   * Partition on a particular cache directory.
   */
  final class DirPartition implements PartitionDescriptor<Integer> {
    private final int mIndex;

    /**
     * Creates a partition based on the directory's index.
     *
     * @param index dir index
     */
    public DirPartition(int index) {
      mIndex = index;
    }

    @Override
    public Integer getIdentifier() {
      return mIndex;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DirPartition that = (DirPartition) o;
      return mIndex == that.mIndex;
    }

    @Override
    public int hashCode() {
      return Objects.hash(mIndex);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("dirIndex", mIndex)
          .toString();
    }
  }

  /**
   * Partition on a cache scope.
   */
  final class ScopePartition implements PartitionDescriptor<CacheScope> {
    private final CacheScope mScope;

    /**
     * Creates a partition over a cache scope.
     *
     * @param scope cache scope
     */
    public ScopePartition(CacheScope scope) {
      mScope = scope;
    }

    @Override
    public CacheScope getIdentifier() {
      return mScope;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ScopePartition that = (ScopePartition) o;
      return Objects.equals(mScope, that.mScope);
    }

    @Override
    public int hashCode() {
      return Objects.hash(mScope);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("scope", mScope)
          .toString();
    }
  }

  /**
   * Partition on a particular file.
   */
  final class FilePartition implements PartitionDescriptor<String> {
    private final String mFileId;

    /**
     * Creates a partition over a file ID.
     *
     * @param fileId the file ID
     */
    public FilePartition(String fileId) {
      mFileId = fileId;
    }

    @Override
    public String getIdentifier() {
      return mFileId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      FilePartition that = (FilePartition) o;
      return Objects.equals(mFileId, that.mFileId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(mFileId);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("fileId", mFileId)
          .toString();
    }
  }
}
