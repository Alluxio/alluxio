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

import java.util.Set;

/**
 * A map-like interface for holding credential properties.
 * */
public interface CredentialProperties {
  /**
   * Gets the value of the credential property key.
   * If the property is not a credential, this should throw a RuntimeException.
   *
   * @param key the property key
   * @return the credential value
   */
  String get(PropertyKey key);

  /**
   * Clears all the credential properties.
   * */
  void clear();

  /**
   * Checks if the credential key is set.
   * If the property is not a credential, this should throw a RuntimeException.
   *
   * @param key the property key
   * @return the credential value
   * */
  boolean containsKey(PropertyKey key);

  /**
   * Sets the credential key.
   * If the property is not a credential, this should throw a RuntimeException.
   *
   * @param key the property key
   * @param value the property value
   * */
  void put(PropertyKey key, String value);

  /**
   * Removes the credential key.
   * If the property is not a credential, this should throw a RuntimeException.
   * If the property key is not set, do nothing.
   *
   * @param key the property key
   * */
  void remove(PropertyKey key);

  /**
   * Returns all credential properties that are set.
   *
   * @return a set of all keys
   * */
  Set<PropertyKey> keySet();
}
