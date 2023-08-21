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

package alluxio.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Registry of all reconfigurable listeners.
 */
@ThreadSafe
public class ReconfigurableRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(ReconfigurableRegistry.class);
  private static final List<Reconfigurable> LISTENER_LIST = new LinkedList<>();

  /**
   * Add a listener.
   *
   * @param listener the given property listener
   */
  public static synchronized void register(Reconfigurable listener) {
    LISTENER_LIST.add(listener);
  }

  /**
   * remove the listener related to the given property.
   * @param listener the listener
   * @return true if the instance is removed
   */
  public static synchronized boolean unregister(Reconfigurable listener) {
    return LISTENER_LIST.remove(listener);
  }

  /**
   * When the property was reconfigured, this function will be invoked.
   * This property listeners will be notified.
   *
   * @return false if no listener related to the given property, otherwise, return false
   */
  public static synchronized boolean update() {
    for (Reconfigurable listener : LISTENER_LIST) {
      try {
        listener.update();
      } catch (Throwable t) {
        LOG.error("Error while update changed properties for {}", listener, t);
      }
    }
    return true;
  }

  // prevent instantiation
  private ReconfigurableRegistry() {}

  /**
   * When the property was reconfigured, this function will be invoked.
   * This property listeners will be notified.
   *
   * @param changedProperties the changed properties
   */
  public static synchronized void update(Map<PropertyKey, Object> changedProperties) {
    for (Reconfigurable listener : LISTENER_LIST) {
      try {
        listener.update(changedProperties);
      } catch (Throwable t) {
        LOG.error("Error while update changed properties {} for {}",
            changedProperties, listener, t);
      }
    }
  }
}
