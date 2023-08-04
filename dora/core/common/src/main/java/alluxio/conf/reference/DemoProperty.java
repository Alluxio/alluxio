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

package alluxio.conf.reference;

import alluxio.conf.PropertyKey;

import java.util.Hashtable;

/**
 * This class is used to demo the reference properties.
 */
public class DemoProperty implements ReferenceProperty {
  private final Hashtable<String, String> mPropertyTable = new Hashtable<String, String>() {
    {
      put(PropertyKey.MASTER_SCHEDULER_INITIAL_DELAY.toString(), "1m");
    }
  };

  @Override
  public Hashtable<String, String> getProperties() {
    return mPropertyTable;
  }
}
