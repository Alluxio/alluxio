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

package alluxio.test.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.testing.EqualsTester;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Common utilities for testing.
 */
public final class CommonUtils {
  private static final Map<Class<?>, List<?>> PRIMITIVE_VALUES =
      ImmutableMap.<Class<?>, List<?>>builder()
      .put(boolean.class, Lists.newArrayList(true, false))
      .put(char.class, Lists.newArrayList('a', 'b'))
      .put(byte.class, Lists.newArrayList((byte) 10, (byte) 11))
      .put(short.class, Lists.newArrayList((short) 20, (short) 21))
      .put(int.class, Lists.newArrayList(30, 31))
      .put(long.class, Lists.newArrayList((long) 40, (long) 41))
      .put(float.class, Lists.newArrayList((float) 50, (float) 51))
      .put(double.class, Lists.newArrayList((double) 60, (double) 61)).build();

  /**
   * Traverses a chain of potentially private fields using {@link Whitebox}.
   *
   * For example, if you have the classes
   *
   *<pre>{@code
   *public class Foo {
   *  private Bar myBar = new Bar();
   *}
   *
   *public class Bar {
   *  private String secret = "puppy";
   *}
   *}</pre>
   *
   * then you can access {@code "puppy"} with
   * {@code CommonTestUtils.getInternalState(new Foo(), "myBar", "secret")}.
   *
   * @param instance the object to start the traversal from
   * @param fieldNames the field names to traverse
   * @return the final value at the end of the traversal
   */
  public static <T> T getInternalState(Object instance, String... fieldNames) {
    Object current = instance;
    for (String fieldName : fieldNames) {
      Object next = Whitebox.getInternalState(current, fieldName);
      if (next == null) {
        throw new RuntimeException(
            "Couldn't find field " + fieldName + " in " + current.getClass());
      }
      current = next;
    }
    @SuppressWarnings("unchecked")
    T finalObject = (T) current;
    return finalObject;
  }

  /**
   * Uses reflection to test the equals and hashCode methods for the given simple java object.
   *
   * It is required that the given class has a no-arg constructor.
   *
   * Note: To use this method to test a class which contains a final non-enum class as a field, the
   * class must either have a no-arg constructor, or you must prepare the final class for testing.
   * See the top of {@link CommonTestUtilsTest} for an example.
   *
   * @param clazz the class to test the equals and hashCode methods for
   * @param excludedFields names of fields which should not impact equality
   */
  public static <T> void testEquals(Class<T> clazz, String... excludedFields) {
    testEquals(clazz, null, null, excludedFields);
  }

  public static <T> void testEquals(Class<T> clazz, Class<?>[] ctorClassArgs,
      Object[] ctorArgs, String... excludedFields) {
    Set<String> excludedFieldsSet = new HashSet<>(Arrays.asList(excludedFields));
    EqualsTester equalsTester = new EqualsTester();
    equalsTester.addEqualityGroup(
        createBaseObject(clazz, ctorClassArgs, ctorArgs),
        createBaseObject(clazz, ctorClassArgs, ctorArgs));
    // For each non-excluded field, create an object of the class with only that field changed.
    for (Field field : getNonStaticFields(clazz)) {
      if (excludedFieldsSet.contains(field.getName())) {
        continue;
      }
      field.setAccessible(true);
      T instance = createBaseObject(clazz, ctorClassArgs, ctorArgs);
      try {
        field.set(instance, getValuesForFieldType(field.getType()).get(1));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      equalsTester.addEqualityGroup(instance);
    }
    equalsTester.testEquals();
  }

  /**
   * Creates new instance of a class by calling a constructor that receives ctorClassArgs arguments.
   *
   * @param <T> type of the object
   * @param clazz the class to create
   * @param ctorClassArgs parameters type list of the constructor to initiate, if null default
   *        constructor will be called
   * @param ctorArgs the arguments to pass the constructor
   * @return new class object
   * @throws RuntimeException if the class cannot be instantiated
   */
  private static <T> T createBaseObject(Class<T> clazz, Class<?>[] ctorClassArgs,
      Object[] ctorArgs) {
    try {
      Constructor<T> ctor;
      if (ctorClassArgs == null || ctorClassArgs.length == 0) {
        ctor = clazz.getDeclaredConstructor();
      } else {
        ctor = clazz.getConstructor(ctorClassArgs);
      }
      ctor.setAccessible(true);

      T instance;
      if (ctorClassArgs == null || ctorClassArgs.length == 0) {
        instance = ctor.newInstance();
      } else {
        instance = ctor.newInstance(ctorArgs);
      }

      for (Field field : getNonStaticFields(clazz)) {
        field.setAccessible(true);
        field.set(instance, getValuesForFieldType(field.getType()).get(0));
      }
      return instance;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns a list of at least two values of the given type.
   *
   * This is done for primitive types by looking them up in a map of preset values. For other types,
   * the first value used is {@code null} and the second value is a mock created by Powermock.
   *
   * @param type the type to return values for
   * @return at least two values assignable to the given type
   */
  private static List<?> getValuesForFieldType(Class<?> type) throws Exception {
    if (type.isEnum()) {
      List<?> enumValues = Lists.newArrayList(type.getEnumConstants());
      enumValues.add(null);
      return enumValues;
    }
    List<?> primitiveValues = PRIMITIVE_VALUES.get(type);
    if (primitiveValues != null) {
      return primitiveValues;
    }
    List<Object> classValues = Lists.newArrayList();
    classValues.add(null);
    try {
      classValues.add(PowerMockito.mock(type));
    } catch (Exception e) {
      try {
        Constructor<?> constructor = type.getDeclaredConstructor();
        constructor.setAccessible(true);
        classValues.add(constructor.newInstance());
      } catch (ReflectiveOperationException e1) {
        throw new RuntimeException(
            "Couldn't create an instance of " + type.getName() + ". Please use @PrepareForTest.");
      }
    }
    return classValues;
  }

  /**
   * @param type the type to get the fields for
   * @return all nonstatic fields of an object of the given type
   */
  private static List<Field> getNonStaticFields(Class<?> type) {
    List<Field> fields = new ArrayList<>();
    for (Class<?> c = type; c != null; c = c.getSuperclass()) {
      for (Field field : c.getDeclaredFields()) {
        if (!Modifier.isStatic(field.getModifiers())) {
          fields.add(field);
        }
      }
    }
    return fields;
  }

  /**
   * Add a path to be loaded.
   * @param path full path
   */
  public static void classLoadURL(String path) throws Exception {
    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    try {
      Method method = classLoader.getClass().getDeclaredMethod("addURL", URL.class);
      method.setAccessible(true);
      method.invoke(classLoader, new File(path).toURI().toURL());
    } catch (NoSuchMethodException e) {
      Method method = classLoader.getClass().getDeclaredMethod(
          "appendToClassPathForInstrumentation", String.class);
      method.setAccessible(true);
      method.invoke(classLoader, path);
    }
  }
}
