/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.resource;

import java.io.Closeable;

/**
 * A {@code CloseableResource<T>} is a wrapper around a resource of type {@code T} which must do
 * some sort of cleanup when it is no longer in use.
 */
public abstract class CloseableResource<T> implements Closeable {
  private T mResource;

  public CloseableResource(T resource) {
    mResource = resource;
  }

  public T get() {
    return mResource;
  }

  public abstract void close();
}
