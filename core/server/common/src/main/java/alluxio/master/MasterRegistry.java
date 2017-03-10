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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for registering individual masters that make up the Alluxio master service. The intended
 * use of this class is for the Alluxio master service to register the individual masters using an
 * instance of this class. The registry is passed as an argument to constructors of individual
 * masters who are expected to add themselves to the registry. The reason for using an instance
 * as opposed to a static class is to enable dependency injection in tests.
 *
 * To prevent race conditions, the contract for this class is that all masters need to be
 * registered (e.g. in constructors) before the registry is used for looking up values. See the
 * {@link MasterRegistry.Value} class for an example on how to perform lazy initialization of
 * variables that need to reference a master.
 */
@ThreadSafe
public final class MasterRegistry {
  private final Map<String, Master> mRegistry = new HashMap<>();

  /**
   * Creates a new instance of {@link MasterRegistry}.
   */
  public MasterRegistry() {}

  /**
   * @param name the name of the master to get
   * @param clazz the class of the master to get
   * @param <T> the type of the master to get
   * @return the master instance, or null if it does not exist or a type mismatch occurs
   */
  public synchronized <T> T get(String name, Class<T> clazz) {
    Master master = mRegistry.get(name);
    if (master == null) {
      return null;
    }
    if (!(clazz.isInstance(master))) {
      return null;
    }
    return clazz.cast(master);
  }

  /**
   * @param name the name of the master to register
   * @param master the master to register
   */
  public synchronized void put(String name, Master master) {
    mRegistry.put(name, master);
  }

  /**
   * @return a collection of all the registered masters
   */
  public synchronized Collection<Master> getMasters() {
    return mRegistry.values();
  }

  /**
   * The purpose of this class is to enable lazy initialization of variables that store a
   * master reference. The intended use is to define these variables in a constructor, but
   * only initialize them when their value is actually needed.
   *
   * For instance, if master A needs a reference to master B, use the following pattern:
   *
   * public class MasterB {
   *   private MasterRegistry.Value<MasterA> mMasterA;
   *
   *   public MasterB(MasterRegistry registry) {
   *     ...
   *     mMasterA = registry.new Value<MasterA>("MasterA", MasterA.class);
   *     ...
   *   }
   *
   *   public void foo() {
   *     // MasterA reference can be obtained here using mMasterA.get()
   *     ...
   *   }
   * }
   *
   * @param <T> the type of the value stored in the class
   */
  @ThreadSafe
  public class Value<T> {
    private Class<T> mClass;
    private T mMaster;
    private String mName;

    /**
     * Creates a new instance of {@link Value} given the master name and type for the value.
     *
     * @param name the master name
     * @param clazz the master class
     */
    public Value(String name, Class<T> clazz) {
      mName = name;
      mClass = clazz;
    }

    /**
     * Performs lazy initialization of the value (if needed) and then returns the typed value.
     *
     * @return the typed value
     */
    public T get() {
      if (mMaster == null) {
        synchronized (this) {
          if (mMaster == null) {
            mMaster = MasterRegistry.this.get(mName, mClass);
          }
        }
      }
      return mMaster;
    }
  }
}
