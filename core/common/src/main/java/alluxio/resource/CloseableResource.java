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

package alluxio.resource;

import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakTracker;

import java.io.Closeable;
import javax.annotation.Nullable;

/**
 * A {@code CloseableResource<T>} is a wrapper around a resource of type {@code T} which must do
 * some sort of cleanup when it is no longer in use.
 *
 * @param <T> the type of the wrapped resource
 */
public abstract class CloseableResource<T> implements Closeable {

  private static final ResourceLeakDetector<CloseableResource> DETECTOR =
      AlluxioResourceLeakDetectorFactory.instance()
          .newResourceLeakDetector(CloseableResource.class);

  private final T mResource;

  @Nullable
  private final ResourceLeakTracker<CloseableResource> mTracker = DETECTOR.track(this);

  /**
   * Creates a {@link CloseableResource} wrapper around the given resource. This resource will
   * be returned by the {@link CloseableResource#get()} method.
   *
   * @param resource the resource to wrap
   */
  public CloseableResource(T resource) {
    mResource = resource;
  }

  /**
   * @return the resource
   */
  public T get() {
    return mResource;
  }

  @Override
  public void close() {
    if (mTracker != null) {
      mTracker.close(this);
    }
    closeResource();
  }

  /**
   * Performs any cleanup operations necessary when the resource is no longer in use.
   */
  public abstract void closeResource();
}
