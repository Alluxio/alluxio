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
import java.util.HashSet;

public final class CredentialConfigItems {
  private static final Logger LOG = LoggerFactory.getLogger(CredentialConfigItems.class);
  public static final HashSet<String>  CREDENTIALS;

  static {
    CREDENTIALS = new HashSet<>();
    try {
      Class clazz = Class.forName("alluxio.conf.PropertyKey");
      Field[] fields = clazz.getFields();
      for (Field field : fields) {
        if (field.getType() == PropertyKey.class && Modifier.isStatic(field.getModifiers())) {
          PropertyKey tmp = (PropertyKey) field.get(null);
          if (tmp.getDisplayType() == PropertyKey.DisplayType.CREDENTIALS) {
            CREDENTIALS.add(tmp.getName());
          }
        }
      }
    } catch (ClassNotFoundException e) {
      LOG.error("can't find alluxio.conf.PropertyKey, can't build credential config items.");
    } catch (IllegalAccessException e) {
      LOG.error("IllegalAccessException:{}", e.toString());
    }
  }
}
