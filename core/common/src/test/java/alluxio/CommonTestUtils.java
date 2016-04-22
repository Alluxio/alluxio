/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.testing.EqualsTester;
import org.junit.Assert;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Common utilities for testing.
 */
public final class CommonTestUtils {
  private static final Map<Class<?>, List<?>> PRIMITIVE_VALUES =
      ImmutableMap.<Class<?>, List<?>>builder()
      .put(boolean.class, Lists.newArrayList(true, false))
      .put(char.class, Lists.newArrayList('a', 'b'))
      .put(byte.class, Lists.newArrayList((byte) 10, (byte) 11))
      .put(short.class, Lists.newArrayList((short) 20, (short) 21))
      .put(int.class, Lists.newArrayList((int) 30, (int) 31))
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
   * Note: To use this method to test a class which contains a final class as a field, you must
   * prepare the final class for testing. See the top of {@link CommonTestUtilsTest} for an example.
   *
   * @param clazz the class to test the equals and hashCode methods for
   * @param excludedFields names of fields which should not impact equality
   */
  public static <T> void testEquals(Class<T> clazz, String... excludedFields) throws Exception {
    Set<String> excludedFieldsSet = Sets.newHashSet(Arrays.asList(excludedFields));
    EqualsTester equalsTester = new EqualsTester();
    equalsTester.addEqualityGroup(createBaseObject(clazz), createBaseObject(clazz));
    // For each non-excluded field, create an object of the class with only that field changed.
    for (Field field : getAllFields(clazz)) {
      if (excludedFieldsSet.contains(field.getName())) {
        continue;
      }
      field.setAccessible(true);
      T instance = createBaseObject(clazz);
      field.set(instance, getValuesForFieldType(field.getType()).get(1));
      equalsTester.addEqualityGroup(instance);
    }
    equalsTester.testEquals();
  }

  /**
   * @param clazz a class
   * @return an object of the given class with fields set according to the first values returned by
   *         {@link #getValuesForFieldType(Class)}
   */
  private static <T> T createBaseObject(Class<T> clazz) throws Exception {
    Constructor<T> constructor = clazz.getDeclaredConstructor();
    constructor.setAccessible(true);
    T instance = constructor.newInstance();
    for (Field field : getAllFields(clazz)) {
      field.setAccessible(true);
      field.set(instance, getValuesForFieldType(field.getType()).get(0));
    }
    return instance;
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
    List<?> values = PRIMITIVE_VALUES.get(type);
    if (values == null) {
      List<Object> list = new ArrayList<Object>();
      list.add(null);
      list.add(PowerMockito.mock(type));
      return list;
    }
    Assert.assertNotNull("Couldn't get values for type " + type, values);
    return values;
  }

  /**
   * @param type the type to get the field for
   * @return all fields of an object of the given type
   */
  private static List<Field> getAllFields(Class<?> type) {
    List<Field> fields = new ArrayList<>();
    for (Class<?> c = type; c != null; c = c.getSuperclass()) {
      fields.addAll(Arrays.asList(c.getDeclaredFields()));
    }
    return fields;
  }
}
