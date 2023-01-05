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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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
import java.util.Objects;
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
  private final Map<Class<? extends Server<U>>, T> mRegistry = new HashMap<>();
  private final Map<Class<? extends Server<U>>, T> mAlias = new HashMap<>();
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
   * @see #getCanonical(Class, int) if alias mappings should be ignored
   */
  public <W extends T> W get(final Class<W> clazz, int timeoutMs) {
    return getInternal(clazz, WaitForOptions.defaults().setTimeoutMs(timeoutMs), true);
  }

  /**
   * Convenience method for {@link #getCanonical(Class, int)} with default timeout.
   *
   * @param type canonical type
   * @return server instance
   * @param <W> server type
   */
  public <W extends T> W getCanonical(final Class<W> type) {
    return getCanonical(type, Constants.DEFAULT_REGISTRY_GET_TIMEOUT_MS);
  }

  /**
   * Gets the server registered with the canonical type.
   * Aliases added via {@link #addAlias(Class, Server)} are not considered.
   *
   * @param type the canonical type
   * @param timeoutMs time before giving up the lookup
   * @return the server instance
   * @param <W> the type
   * @see #get(Class, int)
   */
  public <W extends T> W getCanonical(final Class<W> type, int timeoutMs) {
    return getInternal(type, WaitForOptions.defaults().setTimeoutMs(timeoutMs), false);
  }

  private <W extends T> W getInternal(Class<W> type, WaitForOptions options, boolean includeAlias) {
    try {
      return CommonUtils.waitForResult(
          "server " + type.getName() + " to be created",
          () -> getInternal(type, includeAlias),
          Objects::nonNull,
          options);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  <W extends T> W getInternal(Class<W> clazz, boolean includeAlias) {
    try (LockResource r = new LockResource(mLock)) {
      T server = mRegistry.get(clazz);
      if (server == null && includeAlias) {
        server = mAlias.get(clazz);
      }
      try {
        return clazz.cast(server);
      } catch (ClassCastException e) {
        return null;
      }
    }
  }

  /**
   * Registers a canonical mapping of the type to the server instance. Overrides any existing
   * mappings.
   *
   * @param clazz the class of the {@link Server} to add
   * @param server the {@link Server} to add
   * @param <W> the type of the {@link Server} to add
   * @see #addAlias(Class, Server)
   */
  public <W extends T> void add(Class<W> clazz, T server) {
    Preconditions.checkArgument(clazz.isInstance(server),
        "Server %s is not an instance of %s", server.getClass(), clazz.getName());
    try (LockResource r = new LockResource(mLock)) {
      mRegistry.put(clazz, server);
    }
  }

  /**
   * Registers a server instance under an alias type.
   * <br>
   * Typically, a server instance should be registered with only one type. When
   * it's necessary to register the instance under a super type as well, an alias mapping
   * should be used.
   * The alias mappings are consulted when retrieving the server instances, but
   * are not used to start or stop the server instances.
   *
   * @param clazz the alias type
   * @param server the {@link Server} to add
   * @param <W> the type of the server
   * @throws IllegalArgumentException when the server has not yet been registered under a canonical
   * name with {@link #add(Class, Server)}, or a canonical mapping already exists for the same type
   * @see #add(Class, Server)
   */
  public <W extends T> void addAlias(Class<W> clazz, T server) {
    Preconditions.checkArgument(clazz.isInstance(server),
        "Server %s is not an instance of %s", server.getClass(), clazz.getName());
    try (LockResource r = new LockResource(mLock)) {
      if (mRegistry.containsKey(clazz)) {
        throw new IllegalArgumentException(String.format(
            "Cannot add %s as alias for server %s as the type is already registered as its "
                + "canonical type",
            clazz.getSimpleName(), server.getClass().getSimpleName()));
      }
      if (!mRegistry.containsValue(server)) {
        throw new IllegalArgumentException(String.format(
            "Cannot register alias %s before a canonical name is registered",
            clazz.getSimpleName()));
      }
      mAlias.put(clazz, server);
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
