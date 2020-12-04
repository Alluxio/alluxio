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

package alluxio.master.meta;

import alluxio.conf.PropertyKey;
import alluxio.conf.Hash;
import alluxio.master.journal.DelegatingJournaled;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.Journaled;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.proto.journal.Journal;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.proto.journal.Meta.PathPropertiesEntry;
import alluxio.proto.journal.Meta.RemovePathPropertiesEntry;
import alluxio.resource.CloseableIterator;
import alluxio.resource.LockResource;

import com.google.common.collect.Iterators;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Source of truth for path level properties.
 *
 * We assume that operations for path level properties are not highly concurrent,
 * based on this assumption,
 * thread safety in this class is guaranteed by a read write lock,
 * also for simplicity, for an operation on a path, we journal the full properties of the path
 * after the operation is done.
 *
 * If the assumption is incorrect, we need to use a more fine-grained concurrency control method
 * and only journal the changes to the path properties.
 */
@ThreadSafe
public final class PathProperties implements DelegatingJournaled {
  /** Lock for mState. */
  private final ReadWriteLock mLock = new ReentrantReadWriteLock();
  /** Journaled state of path level properties. */
  @GuardedBy("mLock")
  private final State mState = new State();
  @GuardedBy("mLock")
  private Hash mHash = new Hash(() -> mState.getProperties().entrySet().stream()
      .flatMap(pathProperties -> pathProperties.getValue().entrySet().stream()
          .map(property -> String.format("%s:%s:%s", pathProperties.getKey(), property.getKey(),
              property.getValue()).getBytes())));

  /**
   * @return a snapshot of properties and corresponding hash
   */
  public PathPropertiesView snapshot() {
    try (LockResource r = new LockResource(mLock.readLock())) {
      return new PathPropertiesView(get(), hash());
    }
  }

  /**
   * @return a copy of path level properties which is a map from path to property key values
   */
  public Map<String, Map<String, String>> get() {
    try (LockResource r = new LockResource(mLock.readLock())) {
      return mState.getProperties();
    }
  }

  /**
   * Adds properties for path.
   *
   * If there are existing properties for path, they are merged with the new properties.
   * If a property key already exists, its old value is overwritten.
   *
   * @param ctx the journal context
   * @param path the path
   * @param properties the new properties
   */
  public void add(Supplier<JournalContext> ctx, String path, Map<PropertyKey, String> properties) {
    try (LockResource r = new LockResource(mLock.writeLock())) {
      if (!properties.isEmpty()) {
        Map<String, String> newProperties = mState.getProperties(path);
        properties.forEach((key, value) -> newProperties.put(key.getName(), value));
        mState.applyAndJournal(ctx, PathPropertiesEntry.newBuilder().setPath(path)
            .putAllProperties(newProperties).build());
        mHash.markOutdated();
      }
    }
  }

  /**
   * Removes the specified set of keys from the properties for path.
   *
   * @param ctx the journal context
   * @param path the path
   * @param keys the keys to remove
   */
  public void remove(Supplier<JournalContext> ctx, String path, Set<String> keys) {
    try (LockResource r = new LockResource(mLock.writeLock())) {
      Map<String, String> properties = mState.getProperties(path);
      if (!properties.isEmpty()) {
        keys.forEach(key -> properties.remove(key));
        if (properties.isEmpty()) {
          mState.applyAndJournal(ctx, RemovePathPropertiesEntry.newBuilder().setPath(path).build());
        } else {
          mState.applyAndJournal(ctx, PathPropertiesEntry.newBuilder()
              .setPath(path).putAllProperties(properties).build());
        }
        mHash.markOutdated();
      }
    }
  }

  /**
   * Removes all properties for path.
   *
   * @param ctx the journal context
   * @param path the path
   */
  public void removeAll(Supplier<JournalContext> ctx, String path) {
    try (LockResource r = new LockResource(mLock.writeLock())) {
      Map<String, String> properties = mState.getProperties(path);
      if (!properties.isEmpty()) {
        mState.applyAndJournal(ctx, RemovePathPropertiesEntry.newBuilder().setPath(path).build());
        mHash.markOutdated();
      }
    }
  }

  /**
   * @return the current hash of properties
   */
  public String hash() {
    try (LockResource r = new LockResource(mLock.readLock())) {
      return mHash.get();
    }
  }

  @Override
  public Journaled getDelegate() {
    return mState;
  }

  /**
   * Journaled state of path level properties.
   */
  @NotThreadSafe
  public static final class State implements Journaled {
    /**
     * Map from path to key value properties.
     */
    private final Map<String, Map<String, String>> mProperties = new HashMap<>();

    /**
     * @return a copy of the internal properties
     */
    public Map<String, Map<String, String>> getProperties() {
      Map<String, Map<String, String>> copy = new HashMap<>();
      mProperties.forEach((path, kv) -> copy.put(path, new HashMap<>(kv)));
      return copy;
    }

    /**
     * @param path the path
     * @return a copy of the internal properties for path or empty if configurations for the path
     *    do not exist
     */
    public Map<String, String> getProperties(String path) {
      if (mProperties.containsKey(path)) {
        return new HashMap<>(mProperties.get(path));
      }
      return new HashMap<>();
    }

    @Override
    public boolean processJournalEntry(Journal.JournalEntry entry) {
      if (entry.hasPathProperties()) {
        applyPathProperties(entry.getPathProperties());
      } else if (entry.hasRemovePathProperties()) {
        applyRemovePathProperties(entry.getRemovePathProperties());
      } else {
        return false;
      }
      return true;
    }

    private void applyPathProperties(PathPropertiesEntry entry) {
      String path = entry.getPath();
      Map<String, String> properties = entry.getPropertiesMap();
      if (mProperties.containsKey(path)) {
        mProperties.get(path).clear();
        mProperties.get(path).putAll(properties);
      } else {
        mProperties.put(path, new HashMap<>(properties));
      }
    }

    private void applyRemovePathProperties(RemovePathPropertiesEntry entry) {
      String path = entry.getPath();
      mProperties.remove(path);
    }

    /**
     * @param ctx the journal context
     * @param entry the path properties entry
     */
    public void applyAndJournal(Supplier<JournalContext> ctx, PathPropertiesEntry entry) {
      applyAndJournal(ctx, JournalEntry.newBuilder().setPathProperties(entry).build());
    }

    /**
     * @param ctx the journal context
     * @param entry the remove path properties entry
     */
    public void applyAndJournal(Supplier<JournalContext> ctx, RemovePathPropertiesEntry entry) {
      applyAndJournal(ctx, JournalEntry.newBuilder().setRemovePathProperties(entry).build());
    }

    @Override
    public void resetState() {
      mProperties.clear();
    }

    @Override
    public CheckpointName getCheckpointName() {
      return CheckpointName.PATH_PROPERTIES;
    }

    @Override
    public CloseableIterator<JournalEntry> getJournalEntryIterator() {
      return CloseableIterator.noopCloseable(
          Iterators.transform(mProperties.entrySet().iterator(), entry -> {
            String path = entry.getKey();
            Map<String, String> properties = entry.getValue();
            return Journal.JournalEntry.newBuilder().setPathProperties(PathPropertiesEntry
                .newBuilder().setPath(path).putAllProperties(properties).build()).build();
          }));
    }
  }
}
