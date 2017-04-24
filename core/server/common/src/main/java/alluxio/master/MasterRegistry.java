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

import alluxio.Registry;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A master registry.
 */
@ThreadSafe
public final class MasterRegistry extends Registry<Master, Boolean> {
  /**
   * Creates a new instance of {@link MasterRegistry}.
   */
  public MasterRegistry() {}
<<<<<<< HEAD

  /**
   * Attempts to lookup the master for the given class. Because masters can be looked up and
   * added to the registry in parallel, the lookeup is retried several times before giving up.
   *
   * @param clazz the class of the master to get
   * @param <T> the type of the master to get
   * @return the master instance, or null if a type mismatch occurs
   */
  public <T> T get(Class<T> clazz) {
    Master master;
    try (LockResource r = new LockResource(mLock)) {
      CountingRetry retry = new CountingRetry(RETRY_COUNT);
      while (retry.attemptRetry()) {
        master = mRegistry.get(clazz);
        if (master != null) {
          if (!(clazz.isInstance(master))) {
            return null;
          }
          return clazz.cast(master);
        }
        if (mCondition.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
          // Restart the retry counter when someone woke us up.
          retry = new CountingRetry(RETRY_COUNT);
        }
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    // TODO(jiri): Convert this to a checked exception when exception story is finalized
    throw new RuntimeException(ExceptionMessage.RESOURCE_UNAVAILABLE.getMessage());
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
   * @return a list of all the registered masters, order by dependency relation
   */
  public List<Master> getMasters() {
    List<Master> masters = new ArrayList<>(mRegistry.values());
    Collections.sort(masters, new DependencyComparator());
    return masters;
  }

  /**
   * Starts all masters in dependency order. If A depends on B, A is started before B.
   *
   * If a master fails to start, already-started masters will be stopped.
   *
   * @param isLeader whether to start the masters as leaders
   * @throws IOException if an I/O error occurs
   */
  public void start(boolean isLeader) throws IOException {
    List<Master> started = new ArrayList<>();
    for (Master master : getMasters()) {
      try {
        master.start(isLeader);
        started.add(master);
      } catch (IOException e) {
        for (Master startedMaster: started) {
          startedMaster.stop();
          throw e;
        }
      }
    }
  }

  /**
   * Stops all masters in reverse dependency order. If A depends on B, B is stopped before A.
   *
   * @throws IOException if an I/O error occurs
   */
  public void stop() throws IOException {
    for (Master master : Lists.reverse(getMasters())) {
      master.stop();
    }
  }

  /**
   * Computes a transitive closure of the master dependencies.
   *
   * @param master the master to compute transitive dependencies for
   * @return the transitive dependencies
   */
  private Set<Master> getTransitiveDeps(Master master) {
    Set<Master> result = new HashSet<>();
    Deque<Master> queue = new ArrayDeque<>();
    queue.add(master);
    while (!queue.isEmpty()) {
      Set<Class<?>> deps = queue.pop().getDependencies();
      if (deps == null) {
        continue;
      }
      for (Class<?> clazz : deps) {
        Master dep = mRegistry.get(clazz);
        if (dep == null) {
          continue;
        }
        if (dep.equals(master)) {
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

  /**
   * Used for computing topological sort of masters with respect to the dependency relation.
   */
  private final class DependencyComparator implements Comparator<Master> {
    /**
     * Creates a new instance of {@link DependencyComparator}.
     */
    public DependencyComparator() {}

    @Override
    public int compare(Master left, Master right) {
      Set<Master> leftDeps = getTransitiveDeps(left);
      Set<Master> rightDeps = getTransitiveDeps(right);
      if (leftDeps.contains(right)) {
        return 1;
      }
      if (rightDeps.contains(left)) {
        return -1;
      }
      return left.getName().compareTo(right.getName());
    }
  }
||||||| merged common ancestors

  /**
   * Attempts to lookup the master for the given class. Because masters can be looked up and
   * added to the registry in parallel, the lookeup is retried several times before giving up.
   *
   * @param clazz the class of the master to get
   * @param <T> the type of the master to get
   * @return the master instance, or null if a type mismatch occurs
   */
  public <T> T get(Class<T> clazz) {
    Master master;
    try (LockResource r = new LockResource(mLock)) {
      CountingRetry retry = new CountingRetry(RETRY_COUNT);
      while (retry.attemptRetry()) {
        master = mRegistry.get(clazz);
        if (master != null) {
          if (!(clazz.isInstance(master))) {
            return null;
          }
          return clazz.cast(master);
        }
        if (mCondition.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
          // Restart the retry counter when someone woke us up.
          retry = new CountingRetry(RETRY_COUNT);
        }
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    // TODO(jiri): Convert this to a checked exception when exception story is finalized
    throw new RuntimeException(ExceptionMessage.RESOURCE_UNAVAILABLE.getMessage());
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
   * @return a list of all the registered masters, order by dependency relation
   */
  public List<Master> getMasters() {
    List<Master> masters = new ArrayList<>(mRegistry.values());
    Collections.sort(masters, new DependencyComparator());
    return masters;
  }

  /**
   * Starts all masters in dependency order. If A depends on B, A is started before B.
   *
   * If a master fails to start, already-started masters will be stopped.
   *
   * @param isLeader whether to start the masters as leaders
   * @throws IOException if an IO error occurs
   */
  public void start(boolean isLeader) throws IOException {
    List<Master> started = new ArrayList<>();
    for (Master master : getMasters()) {
      try {
        master.start(isLeader);
        started.add(master);
      } catch (IOException e) {
        for (Master startedMaster: started) {
          startedMaster.stop();
          throw e;
        }
      }
    }
  }

  /**
   * Stops all masters in reverse dependency order. If A depends on B, B is stopped before A.
   *
   * @throws IOException if an IO error occurs
   */
  public void stop() throws IOException {
    for (Master master : Lists.reverse(getMasters())) {
      master.stop();
    }
  }

  /**
   * Computes a transitive closure of the master dependencies.
   *
   * @param master the master to compute transitive dependencies for
   * @return the transitive dependencies
   */
  private Set<Master> getTransitiveDeps(Master master) {
    Set<Master> result = new HashSet<>();
    Deque<Master> queue = new ArrayDeque<>();
    queue.add(master);
    while (!queue.isEmpty()) {
      Set<Class<?>> deps = queue.pop().getDependencies();
      if (deps == null) {
        continue;
      }
      for (Class<?> clazz : deps) {
        Master dep = mRegistry.get(clazz);
        if (dep == null) {
          continue;
        }
        if (dep.equals(master)) {
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

  /**
   * Used for computing topological sort of masters with respect to the dependency relation.
   */
  private final class DependencyComparator implements Comparator<Master> {
    /**
     * Creates a new instance of {@link DependencyComparator}.
     */
    public DependencyComparator() {}

    @Override
    public int compare(Master left, Master right) {
      Set<Master> leftDeps = getTransitiveDeps(left);
      Set<Master> rightDeps = getTransitiveDeps(right);
      if (leftDeps.contains(right)) {
        return 1;
      }
      if (rightDeps.contains(left)) {
        return -1;
      }
      return left.getName().compareTo(right.getName());
    }
  }
=======
>>>>>>> upstream/master
}
