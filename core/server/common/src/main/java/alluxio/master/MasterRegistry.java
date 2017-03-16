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

import alluxio.resource.LockResource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for registering individual masters that make up the Alluxio master service. The intended
 * use of this class is for the Alluxio master service to register the individual masters using an
 * instance of this class. The registry is passed as an argument to constructors of individual
 * masters who are expected to add themselves to the registry. The reason for using an instance
 * as opposed to a static class is to enable dependency injection in tests.
 */
// TODO(jiri): Detect deadlocks.
@ThreadSafe
public final class MasterRegistry {
  /**
   * Used for enforcing the order in which masters are iterated over. Currently, the only
   * requirement is that block master appears before file system master and the comparator thus
   * uses alphabetical ordering.
   */
  private static final Comparator<Master> COMPARATOR = new Comparator<Master>() {
    @Override
    public int compare(Master left, Master right) {
      return left.getName().compareTo(right.getName());
    }
  };

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
   * @return the master instance, or null if it does not exist or a type mismatch occurs
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
   * @param clazz the class of the master to get
   * @param master the master to register
   * @param <T> the type of the master to get
   */
  public <T> void put(Class<T> clazz, Master master) {
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
