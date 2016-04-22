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

import alluxio.util.CommonUtils;

import com.google.common.base.Function;
import org.powermock.reflect.Whitebox;

/**
 * Common utilities for testing.
 */
public final class CommonTestUtils {

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
   * Waits for a condition to be satisfied until a timeout occurs.
   *
   * @param condition the condition to wait on
   * @param timeoutMs the number of milliseconds to wait before giving up and throwing an exception
   */
  public static void waitFor(Function<Void, Boolean> condition, int timeoutMs) {
    long start = System.currentTimeMillis();
    while (!condition.apply(null)) {
      if (System.currentTimeMillis() - start > timeoutMs) {
        throw new RuntimeException("Timed out waiting for condition " + condition);
      }
      CommonUtils.sleepMs(20);
    }
  }

  /**
   * Waits for the condition to be satisfied using the default timeout.
   *
   * @param condition the condition to wait on
   */
  public static void waitFor(Function<Void, Boolean> condition) {
    waitFor(condition, 5 * Constants.SECOND_MS);
  }
}
