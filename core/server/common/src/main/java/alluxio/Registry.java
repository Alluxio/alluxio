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

package alluxio;

import alluxio.resource.LockResource;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for registering individual {@link Server}s that run within an Alluxio process. The intended
 * use of this class is for the Alluxio process to register the individual {@link Server}s using an
 * instance of this class. The registry is passed as an argument to constructors of individual
 * {@link Server}s who are expected to add themselves to the registry.
 *
 * The reason for using an instance as opposed to a static class is to enable dependency
 * injection in tests.
 *
 * @param <T> the type of the {@link Server}
 * @param <U> the type of the {@link Server#start} method options
 */
@ThreadSafe
public class Registry<T extends Server<U>, U> {
  private final Map<Class<? extends Server>, T> mRegistry = new HashMap<>();
  private final Lock mLock = new ReentrantLock();

  /**
   * Creates a new instance of {@link Registry}.
   */
  public Registry() {}

  /**
   * Convenience method for calling {@link #get(Class, int)} with a default timeout.
   *
   * @param clazz the class of the {@link Server} to get
   * @param <W> the type of the {@link Server} to get
   * @return the {@link Server} instance
   */
  public <W extends T> W get(final Class<W> clazz) {
    return get(clazz, Constants.DEFAULT_REGISTRY_GET_TIMEOUT_MS);
  }

  /**
   * Attempts to look up the {@link Server} for the given class. Because {@link Server}s can be
   * looked up and added to the registry in parallel, the lookup is retried until the given timeout
   * period has elapsed.
   *
   * @param clazz the class of the {@link Server} to get
   * @param timeoutMs timeout for looking up the server
   * @param <W> the type of the {@link Server} to get
   * @return the {@link Server} instance
   */
  public <W extends T> W get(final Class<W> clazz, int timeoutMs) {
    try {
      CommonUtils.waitFor("server " + clazz.getName() + " to be created", () -> {
        try (LockResource r = new LockResource(mLock)) {
          return mRegistry.get(clazz) != null;
        }
      }, WaitForOptions.defaults().setTimeoutMs(timeoutMs));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    }
    T server = mRegistry.get(clazz);
    if (!(clazz.isInstance(server))) {
      throw new RuntimeException("Server is not an instance of " + clazz.getName());
    }
    return clazz.cast(server);
  }

  /**
   * @param clazz the class of the {@link Server} to add
   * @param server the {@link Server} to add
   * @param <W> the type of the {@link Server} to add
   */
  public <W extends T> void add(Class<W> clazz, T server) {
    try (LockResource r = new LockResource(mLock)) {
      mRegistry.put(clazz, server);
    }
  }

  /**
   * @return a list of all the registered {@link Server}s, order by dependency relation
   */
  public List<T> getServers() {
    List<T> servers = new ArrayList<>(mRegistry.values());
    servers.sort(new DependencyComparator());
    return servers;
  }

  /**
   * Starts all {@link Server}s in dependency order. If A depends on B, A is started before B.
   *
   * If a {@link Server} fails to start, already-started {@link Server}s will be stopped.
   *
   * @param options the start options
   */
  public void start(U options) throws IOException {
    List<T> servers = new ArrayList<>();
    for (T server : getServers()) {
      try {
        server.start(options);
        servers.add(server);
      } catch (IOException e) {
        for (T started : servers) {
          started.stop();
        }
        throw e;
      }
    }
  }

  /**
   * Stops all {@link Server}s in reverse dependency order. If A depends on B, A is stopped
   * before B.
   */
  public void stop() throws IOException {
    for (T server : Lists.reverse(getServers())) {
      server.stop();
    }
  }

  /**
   * Closes all {@link Server}s in reverse dependency order. If A depends on B, A is closed
   * before B.
   */
  public void close() throws IOException {
    for (T server : Lists.reverse(getServers())) {
      server.close();
    }
  }

  /**
   * Computes a transitive closure of the {@link Server} dependencies.
   *
   * @param server the {@link Server} to compute transitive dependencies for
   * @return the transitive dependencies
   */
  private Set<T> getTransitiveDeps(T server) {
    Set<T> result = new HashSet<>();
    Deque<T> queue = new ArrayDeque<>();
    queue.add(server);
    while (!queue.isEmpty()) {
      Set<Class<? extends Server>> deps = queue.pop().getDependencies();
      if (deps == null) {
        continue;
      }
      for (Class<? extends Server> clazz : deps) {
        T dep = mRegistry.get(clazz);
        if (dep == null) {
          continue;
        }
        if (dep.equals(server)) {
          throw new RuntimeException("Dependency cycle encountered");
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
   * Used for computing topological sort of {@link Server}s with respect to the dependency relation.
   */
  private final class DependencyComparator implements Comparator<T> {
    /**
     * Creates a new instance of {@link DependencyComparator}.
     */
    DependencyComparator() {}

    @Override
    public int compare(T left, T right) {
      Set<T> leftDeps = getTransitiveDeps(left);
      Set<T> rightDeps = getTransitiveDeps(right);
      if (leftDeps.contains(right)) {
        return 1;
      }
      if (rightDeps.contains(left)) {
        return -1;
      }
      return left.getName().compareTo(right.getName());
    }
  }
}
