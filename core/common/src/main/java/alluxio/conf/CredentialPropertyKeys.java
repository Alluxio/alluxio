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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 *  This class stores names of property keys whose values are credential.
 */
public final class CredentialPropertyKeys {
  private static final Logger LOG = LoggerFactory.getLogger(CredentialPropertyKeys.class);
  private static final Set<String> CREDENTIALS;

  static {
    CREDENTIALS = findCredentialPropertyKeys("alluxio.conf.PropertyKey");
  }

  protected static Set<String> findCredentialPropertyKeys(String propertyClass) {
    Set<String> credentials = new HashSet<>();
    try {
      Class clazz = Class.forName(propertyClass);
      Field[] fields = clazz.getFields();
      for (Field field : fields) {
        if (field.getType() == PropertyKey.class && Modifier.isStatic(field.getModifiers())) {
          PropertyKey tmp = (PropertyKey) field.get(null);
          if (tmp.getDisplayType() == PropertyKey.DisplayType.CREDENTIALS) {
            credentials.add(tmp.getName());
          }
        }
      }
    } catch (ClassNotFoundException | IllegalAccessException e) {
      LOG.error("Failed to parse class alluxio.conf.PropertyKey", e);
    }

    return Collections.unmodifiableSet(credentials);
  }

  /**
   * return CREDENTIAL set.
   * @return CREDENTIALS
   */
  public static Set<String> getCredentials() {
    return CREDENTIALS;
  }
}
