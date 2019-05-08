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

import java.lang.annotation.Annotation;
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
 * The class is mainly useful for {@link Deprecated} annotations, but could add more in the future.
 *
 * @param <T> The type of annotation this checker should be used for
 */
public class AnnotatedKeyChecker<T extends Annotation> {

  private Map<PropertyKey, T> mAnnotatedKeys;
  private Map<PropertyKey.Template, T> mAnnotatedTemplates;
  private final Class<?> mKeySearchClass;
  private final Class<?> mTemplateSearchClass;
  private final Class<T> mAnnotationClass;
  private AtomicBoolean mInitialized = new AtomicBoolean(false);

  /**
   * Create a new instance of {@link AnnotatedKeyChecker}.
   *
   * @param keySearchClass the class to search for {@link PropertyKey}s
   * @param templateSearchClass the class to search for {@link alluxio.conf.PropertyKey.Template}
   * @param annotationClass the class of the annotation
   */
  public AnnotatedKeyChecker(Class<?> keySearchClass,
      Class<?> templateSearchClass, Class<T> annotationClass) {
    mKeySearchClass = keySearchClass;
    mTemplateSearchClass = templateSearchClass;
    mAnnotationClass = annotationClass;
  }

  /**
   * Given a class to search, a field type, and an annotation type will return a map of all
   * fields which are marked with the given annotation to the instance of the annotation.
   *
   * @param searchClass the class to search through for fields
   * @param fieldType the class of the field to search for
   * @param annotationClazz the annotation to look for
   * @param <I> The class to search through for annotatated fields
   * @param <J> The class of the field to look for
   * @param <K> a class extending Annotation
   * @return a map
   */
  private static <I, J, K extends Annotation> Map<J, K> populateAnnotatedKeyMap(
      Class<I> searchClass, Class<J> fieldType, Class<K> annotationClazz) {
    Map<J, K> annotations = new HashMap<>();
    for (Field field : searchClass.getDeclaredFields()) {
      if (!field.getType().equals(fieldType)) {
        continue;
      }

      K keyAnnotation = field.getAnnotation(annotationClazz);

      try {
        // Field#get parameter can be null if retrieving a static field (all PKs are static)
        // See https://docs.oracle.com/javase/8/docs/api/java/lang/reflect/Field.html
        // This also works with Template enums
        J key = fieldType.cast(field.get(null));
        if (keyAnnotation != null) {
          annotations.put(key, keyAnnotation);
        }
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
    return annotations;
  }

  private T getKeyAnnotation(PropertyKey key) {
    if (!mInitialized.get()) {
      initialize();
    }
    if (mAnnotatedKeys.containsKey(key)) {
      return mAnnotatedKeys.get(key);
    } else {
      for (Map.Entry<PropertyKey.Template, T> e : mAnnotatedTemplates.entrySet()) {
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
          populateAnnotatedKeyMap(mKeySearchClass, PropertyKey.class, mAnnotationClass);
      mAnnotatedTemplates =
          populateAnnotatedKeyMap(mTemplateSearchClass, PropertyKey.Template.class,
              mAnnotationClass);
    }
  }

  /**
   * Returns whether or not the given property key is marked by this class' annotation.
   *
   * It first checks if the specific key has the annotation, otherwise it will fall back to checking
   * if the key's name matches any of the PropertyKey templates. If no keys or templates match, it
   * will return false. This will only return true when the key is marked with a {@link T}
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
  public T getAnnotation(PropertyKey key) {
    return getKeyAnnotation(key);
  }
}
