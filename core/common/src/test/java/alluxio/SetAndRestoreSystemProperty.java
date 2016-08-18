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

/**
 * An AutoCloseable which temporarily modifies a system property when it is constructed and
 * restores the property when it is closed.
 */
public final class SetAndRestoreSystemProperty implements AutoCloseable {
  private final String mPropertyName;
  private final String mPreviousValue;

  /**
   * @param propertyName the name of the property to set
   * @param value the value to set it to
   */
  public SetAndRestoreSystemProperty(String propertyName, String value) {
    mPropertyName = propertyName;
    mPreviousValue = System.getProperty(propertyName);
    System.setProperty(mPropertyName, value);
  }

  @Override
  public void close() throws Exception {
    if (mPreviousValue == null) {
      System.clearProperty(mPropertyName);
    } else {
      System.setProperty(mPropertyName, mPreviousValue);
    }
  }
}
