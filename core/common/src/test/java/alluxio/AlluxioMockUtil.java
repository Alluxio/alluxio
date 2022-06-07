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

import org.apache.commons.lang3.reflect.FieldUtils;

import java.lang.reflect.Field;

public class AlluxioMockUtil {

  /**
   * Replace `Whitebox.setInternalState` method in PowerMockito.
   * Set the value of a field using reflection.
   */
  public static void setInternalState(Object object, String fieldName, Object value) {
    try {
      Class<?> cls = getType(object);
      Field field = cls.getDeclaredField(fieldName);
      FieldUtils.removeFinalModifier(field);
      FieldUtils.writeField(field, object, value, true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Replace `Whitebox.getInternalState` method in PowerMockito.
   * Get the value of a field using reflection.
   */
  public static <T> T getInternalState(Object object, String fieldName) {
    try {
      Class<?> cls = getType(object);
      Field field = cls.getDeclaredField(fieldName);
      field.setAccessible(true);
      return (T) field.get(object);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Gets the type.
   *
   * @param object the object
   * @return The type of the of an object
   */
  public static Class<?> getType(Object object) {
    Class<?> type = null;
    if (object instanceof Class<?>) {
      type = (Class<?>) object;
    } else if (object != null) {
      type = object.getClass();
    }
    return type;
  }
}
