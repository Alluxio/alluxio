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

package alluxio.master.transport;

import java.util.function.Consumer;

/**
 * Context for unregistering a registered listener.
 *
 * The listener context represents a registered listener.
 * The context is normally returned when a {@link Consumer} is
 * registered and can be used to unregister the listener via {@link #close()}.
 *
 * @param <T> event type
 */
public interface Listener<T> extends Consumer<T>, AutoCloseable {
  /**
   * Closes the listener.
   * When the listener is closed, the listener will be unregistered
   * and will no longer receive events for which it was listening.
   */
  void close();
}
