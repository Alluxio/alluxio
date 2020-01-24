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

package alluxio.client.block.stream;

import alluxio.resource.CloseableResource;

/**
 * A closable resource that does nothing when {@link #close()} is called.
 *
 * @param <T> closable type
 */
public class NoopClosableResource<T> extends CloseableResource<T> {

  /**
   * Creates a {@link CloseableResource} wrapper around the given resource. This resource will
   * be returned by the {@link CloseableResource#get()} method.
   *
   * @param resource the resource to wrap
   */
  public NoopClosableResource(T resource) {
    super(resource);
  }

  @Override
  public void close() {
  }
}
