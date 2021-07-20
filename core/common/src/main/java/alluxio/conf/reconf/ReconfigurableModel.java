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

package alluxio.conf.reconf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * ReconfigurableModel manager listeners and fire the event.
 */
public class ReconfigurableModel {
  public static final Log LOG = LogFactory.getLog(ReconfigurableModel.class);
  private static final ReconfigurableModel INSTANCE = new ReconfigurableModel();
  private List<ReconfigurableListener> mListenerList = new LinkedList<>();

  /**
   * @return the instance
   */
  public static ReconfigurableModel getInstance() {
    return INSTANCE;
  }

  /**
   * Add a listener.
   *
   * @param listener the given property listener
   * @return return current instance
   */
  public synchronized ReconfigurableModel addListener(ReconfigurableListener listener) {
    mListenerList.add(listener);
    return INSTANCE;
  }

  /**
   * remove the listener related to the given property.
   * @param listener the listener
   * @return the instance
   */
  public synchronized ReconfigurableModel removeListener(
      ReconfigurableListener listener) {
    mListenerList.remove(listener);
    return INSTANCE;
  }

  /**
   * When the property was reconfigured, this function will be invoked.
   * This property listeners will be notified.
   *
   * @return false if no listener related to the given property, otherwise, return false
   */
  public synchronized boolean propertyChange() {
    for (ReconfigurableListener listener : mListenerList) {
      listener.propertyChange();
    }
    return true;
  }
}
