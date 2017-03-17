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

package alluxio.master;

import alluxio.exception.ExceptionMessage;
import alluxio.resource.LockResource;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for registering individual masters that make up the Alluxio master service. The intended
 * use of this class is for the Alluxio master service to register the individual masters using an
 * instance of this class. The registry is passed as an argument to constructors of individual
 * masters who are expected to add themselves to the registry.
 *
 * The reason for using an instance as opposed to a static class is to enable dependency
 * injection in tests.
 */
@ThreadSafe
public final class MasterRegistry {
  /**
   * Records dependencies between masters stored in the registry. In particular, DEPS[x] records
   * the set of masters that the master X depends on. The dependencies are respected by
   * {@link #getMasters()} which determines the order in which masters are iterated over.
   */
  private final Map<Class<?>, Set<Class<?>>> mDeps = new HashMap<>();
  private final Map<Class<?>, Master> mRegistry = new HashMap<>();
  private final Lock mLock = new ReentrantLock();
  private final Condition mCondition = mLock.newCondition();

  /**
   * Creates a new instance of {@link MasterRegistry}.
   */
  public MasterRegistry() {}

  /**
   * @param clazz the class of the master to get
   * @param <T> the type of the master to get
   * @return the master instance, or null if a type mismatch occurs
   */
  public <T> T get(Class<T> clazz) {
    Master master;
    try (LockResource r = new LockResource(mLock)) {
      while (true) {
        master = mRegistry.get(clazz);
        if (master != null) {
          break;
        }
        mCondition.awaitUninterruptibly();
      }
      if (!(clazz.isInstance(master))) {
        return null;
      }
      return clazz.cast(master);
    }
  }

  /**
   * @param clazz the class of the master to add
   * @param master the master to add
   * @param <T> the type of the master to add
   */
  public <T> void add(Class<T> clazz, Master master) {
    try (LockResource r = new LockResource(mLock)) {
      mDeps.put(clazz, master.getDependencies());
      mRegistry.put(clazz, master);
      mCondition.signalAll();
    }
  }

  /**
   * @return a collection of all the registered masters
   */
  public Collection<Master> getMasters() {
    List<Map.Entry<Class<?>, Master>> entries = new ArrayList<>(mRegistry.entrySet());
    Collections.sort(entries, new DependencyComparator());
    List<Master> masters = new ArrayList<>();
    for (Map.Entry<Class<?>, Master> entry : entries) {
      masters.add(entry.getValue());
    }
    return masters;
  }

  private Set<Class<?>> getTransitiveDeps(Class<?> clazz) {
    Set<Class<?>> result = new HashSet<>();
    Deque<Class<?>> queue = new ArrayDeque<>();
    queue.add(clazz);
    while (!queue.isEmpty()) {
      Set<Class<?>> deps = mDeps.get(queue.pop());
      if (deps == null) {
        continue;
      }
      for (Class<?> dep : deps) {
        if (dep.equals(clazz)) {
          throw new RuntimeException(ExceptionMessage.DEPENDENCY_CYCLE.getMessage());
        }
        if (result.contains(dep)) {
          continue;
        }
        queue.add(dep);
        result.add(dep);
      }
    }
    return result;
  }

  private final class DependencyComparator implements Comparator<Map.Entry<Class<?>, Master>> {
    public DependencyComparator() {}

    @Override
    public int compare(Map.Entry<Class<?>, Master> left, Map.Entry<Class<?>, Master> right) {
      Set<Class<?>> leftDeps = getTransitiveDeps(left.getKey());
      Set<Class<?>> rightDeps = getTransitiveDeps(right.getKey());
      if (leftDeps.contains(right.getKey())) {
        return 1;
      }
      if (rightDeps.contains(left.getKey())) {
        return -1;
      }
      return left.getValue().getName().compareTo(right.getValue().getName());
    }
  }
}
