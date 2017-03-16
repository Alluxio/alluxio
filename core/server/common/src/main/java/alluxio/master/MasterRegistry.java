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

import alluxio.Constants;
import alluxio.resource.LockResource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
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
   * Used for enforcing the order in which masters are iterated over. If there is a known
   * dependency between the masters, then the comparator respects the dependency. Otherwise, it
   * uses alphabetical ordering.
   */
  private static final Comparator<Master> COMPARATOR = new Comparator<Master>() {
    @Override
    public int compare(Master left, Master right) {
      Set<String> deps = DEPS.get(left.getName());
      if (deps != null && deps.contains(right.getName())) {
        return -1;
      }
      return left.getName().compareTo(right.getName());
    }
  };

  /**
   * Records dependencies between masters stored in the registry. In particular, DEPS[x] records
   * the set of masters that the master X depends on. The dependencies are respected by
   * {@link #getMasters()} which determines the order in which masters are iterated over.
   */
  private static final Map<String, Set<String>> DEPS = new HashMap<>();

  static {
    // The block master depends on the file system master.
    Set<String> blockMasterDeps = new HashSet<>();
    blockMasterDeps.add(Constants.FILE_SYSTEM_MASTER_NAME);
    DEPS.put(Constants.BLOCK_MASTER_NAME, blockMasterDeps);
  }

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
      mRegistry.put(clazz, master);
      mCondition.signalAll();
    }
  }

  /**
   * @return a collection of all the registered masters
   */
  public synchronized Collection<Master> getMasters() {
    List<Master> masters = new ArrayList<>(mRegistry.values());
    Collections.sort(masters, COMPARATOR);
    return masters;
  }
}
