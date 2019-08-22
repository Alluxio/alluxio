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

import alluxio.exception.ExceptionMessage;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The default implementation of CredentialProperties that stores and returns credential
 * properties in plaintext.
 */
public class DefaultCredentialProperties implements CredentialProperties {
  private final ConcurrentHashMap<PropertyKey, String> mProps = new ConcurrentHashMap<>();

  /**
   * Creates a new instance of {@link DefaultCredentialProperties}.
   * */
  public DefaultCredentialProperties() {}

  /**
   * Creates a new instance of {@link DefaultCredentialProperties} copying an existing one.
   *
   * @param properties the {@link CredentialProperties} to copy
   * */
  public DefaultCredentialProperties(CredentialProperties properties) {
    for (PropertyKey key : properties.keySet()) {
      mProps.put(key, properties.get(key));
    }
  }

  /**
   * Gets the value of the credential property key.
   * An exception will be thrown if the property is not already set.
   *
   * @param key the property key
   * @return the credential value
   */
  @Override
  public String get(PropertyKey key) {
    validateKey(key);
    if (!mProps.containsKey(key)) {
      throw new RuntimeException(ExceptionMessage.UNDEFINED_CONFIGURATION_KEY.getMessage(key));
    }
    return mProps.get(key);
  }

  /**
   * Clears all the credential properties
   * */
  @Override
  public void clear() {
    mProps.clear();
  }

  /**
   * Checks if the credential key is set.
   * If the property is not a credential, this should throw a RuntimeException.
   *
   * @param key the property key
   * @return the credential value
   * */
  @Override
  public boolean containsKey(PropertyKey key) {
    validateKey(key);
    return mProps.containsKey(key);
  }

  /**
   * Sets the credential key.
   * If the property is not a credential, this should throw a RuntimeException.
   *
   * @param key the property key
   * @param value the property value
   * */
  @Override
  public void put(PropertyKey key, String value) {
    validateKey(key);
    mProps.put(key, value);
  }

  /**
   * Removes the credential key.
   * If the property is not a credential, this should throw a RuntimeException.
   * If the property key is not set, do nothing.
   *
   * @param key the property key
   * */
  @Override
  public void remove(PropertyKey key) {
    validateKey(key);
    mProps.remove(key);
  }

  /**
   * Returns all credential properties that are set.
   *
   * @return the set of all keys
   * */
  @Override
  public Set<PropertyKey> keySet() {
    return mProps.keySet();
  }

  private void validateKey(PropertyKey key) {
    if (!key.isCredential()) {
      throw new RuntimeException(key.toString() + " is not a credential field!");
    }
  }
}
