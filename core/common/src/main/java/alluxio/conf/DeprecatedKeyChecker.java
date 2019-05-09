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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;

import javax.annotation.Nullable;

/**
 * This annotation checker should be used to determine whether a {@link PropertyKey} or
 * {@link alluxio.conf.PropertyKey.Template} has a given annotation.
 *
 * The class is mainly useful to check for {@link Deprecated} annotations on property keys and
 * return the associated message, if any.
 */
public class DeprecatedKeyChecker {
  private static final Class<PropertyKey.Template> TEMPLATE_CLASS =
      PropertyKey.Template.class;
  private static final Class<PropertyKey> KEY_CLASS = PropertyKey.class;

  private Map<PropertyKey, Deprecated> mAnnotatedKeys;
  private Map<PropertyKey.Template, Deprecated> mAnnotatedTemplates;
  private AtomicBoolean mInitialized = new AtomicBoolean(false);

  /**
   * Create a new instance of {@link DeprecatedKeyChecker}.
   */
  public DeprecatedKeyChecker() { }

  /**
   * Given a class to search, a field type, and an annotation type will return a map of all
   * fields which are marked with the given annotation to the instance of the annotation.
   *
   * @param searchType the class to search through of the given type
   * @param <T> The type of the field to retrieve
   * @return a map of all fields within {@code searchType} class of the type T that are annotated
   *         with the {@link Deprecated} annotation
   */
  private static <T> Map<T, Deprecated> populateAnnotatedKeyMap(Class<T> searchType) {
    Map<T, Deprecated> annotations = new HashMap<>();
    // Iterate over all fields in the class
    for (Field field : searchType.getDeclaredFields()) {
      // If the field isn't equal to the class type, skip it
      if (!field.getType().equals(searchType)) {
        continue;
      }

      Deprecated keyAnnotation = field.getAnnotation(Deprecated.class);

      try {
        // Field#get parameter can be null if retrieving a static field (all PKs are static)
        // See https://docs.oracle.com/javase/8/docs/api/java/lang/reflect/Field.html
        // This also works with Template enums
        T key = searchType.cast(field.get(null));
        if (keyAnnotation != null) {
          annotations.put(key, keyAnnotation);
        }
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
    return annotations;
  }

  private Deprecated getKeyAnnotation(PropertyKey key) {
    if (!mInitialized.get()) {
      initialize();
    }
    if (mAnnotatedKeys.containsKey(key)) {
      return mAnnotatedKeys.get(key);
    } else {
      for (Map.Entry<PropertyKey.Template, Deprecated> e : mAnnotatedTemplates.entrySet()) {
        Matcher match = e.getKey().match(key.getName());
        if (match.matches()) {
          return e.getValue();
        }
      }
    }
    return null;
  }

  private void initialize() {
    if (!mInitialized.getAndSet(true)) {
      mAnnotatedKeys =
          populateAnnotatedKeyMap(KEY_CLASS);
      mAnnotatedTemplates =
          populateAnnotatedKeyMap(TEMPLATE_CLASS);
    }
  }

  /**
   * Returns whether or not the given property key is marked by this class' annotation.
   *
   * It first checks if the specific key has the annotation, otherwise it will fall back to checking
   * if the key's name matches any of the PropertyKey templates. If no keys or templates match, it
   * will return false. This will only return true when the key is marked with a {@link Deprecated}
   * annotation.
   *
   * @param key the property key to check
   * @return if this property key is deprecated
   * @see Deprecated
   * @see PropertyKey#getDeprecationMessage(PropertyKey)
   */
  public boolean hasAnnotation(PropertyKey key) {
    return getKeyAnnotation(key) != null;
  }

  /**
   * Returns the annotation attached to the given property key.
   *
   * @param key the key to get the annotation for
   * @return the annotation if it exists, null otherwise
   */
  @Nullable
  public Deprecated getAnnotation(PropertyKey key) {
    return getKeyAnnotation(key);
  }
}
