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

import alluxio.underfs.UnderFileSystem.SpaceType;

import junit.framework.AssertionFailedError;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Objects;

/**
 * Unit tests for {@link CommonUtils}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(CommonTestUtilsTest.HardToInstantiateClass.class)
public final class CommonTestUtilsTest {
  public static final class HardToInstantiateClass {
    private HardToInstantiateClass(Object o) {}
  }

  /**
   * A simple class that has a correct hashcode method.
   */
  private static class Basic {
    private String mField;

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Basic)) {
        return false;
      }
      Basic that = (Basic) o;
      return Objects.equals(mField, that.mField);
    }

    @Override
    public int hashCode() {
      return Objects.hash(mField);
    }
  }

  /**
   * A simple class that has a bad hashcode method.
   */
  private static class BadHashCode {
    private String mField;

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Basic)) {
        return false;
      }
      Basic that = (Basic) o;
      return Objects.equals(mField, that.mField);
    }

    @Override
    public int hashCode() {
      return Objects.hash(10); // Should be mField.
    }
  }

  /**
   * A simple class that has a correct equals method.
   */
  private static class ManyFields {
    private String mField1;
    private boolean mField2;
    // Use HardToInstantiateClass as an example of a final class without a no-arg constructor.
    private HardToInstantiateClass mField3;
    // Example of an enum.
    private SpaceType mField4;

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof ManyFields)) {
        return false;
      }
      ManyFields that = (ManyFields) o;
      return Objects.equals(mField1, that.mField1)
          && Objects.equals(mField2, that.mField2)
          && Objects.equals(mField3, that.mField3)
          && Objects.equals(mField4, that.mField4);
    }

    @Override
    public int hashCode() {
      return Objects.hash(mField1, mField2, mField3, mField4);
    }
  }

  /**
   * A simple class that has a bad equals method due to missing field.
   */
  private static class MissingField {
    private String mField1;
    private boolean mField2;
    private CommonUtils mField3;

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof MissingField)) {
        return false;
      }
      MissingField that = (MissingField) o;
      // Missing test for mField3.
      return Objects.equals(mField1, that.mField1)
          && Objects.equals(mField2, that.mField2);
    }

    @Override
    public int hashCode() {
      // Missing mField3.
      return Objects.hash(mField1, mField2);
    }
  }

  /**
   * A simple class that has a bad equals method due to missing instance check.
   */
  private static class EqualsMissingInstanceCheck {
    private String mField1;
    private boolean mField2;
    private CommonUtils mField3;

    @Override
    public boolean equals(Object o) {
      // Missing test for o instanceof EqualsMissingField.
      MissingField that = (MissingField) o;

      return Objects.equals(mField1, that.mField1)
          && Objects.equals(mField2, that.mField2)
          && Objects.equals(mField3, that.mField3);
    }

    @Override
    public int hashCode() {
      return Objects.hash(mField1, mField2, mField3);
    }
  }

  @Test
  public void testEqualsBasic() throws Exception {
    CommonUtils.testEquals(Basic.class);
  }

  @Test
  public void testEqualsManyFields() {
    CommonUtils.testEquals(ManyFields.class);
  }

  @Test
  public void testEqualsBadHashCodeCheck() {
    testFail(BadHashCode.class);
  }

  @Test
  public void testEqualsMissingFieldCheck() {
    testFail(MissingField.class);
  }

  @Test
  public void testEqualsMissingInstanceCheck() {
    testFail(EqualsMissingInstanceCheck.class);
  }

  private void testFail(Class<?> clazz) {
    try {
      CommonUtils.testEquals(clazz);
      Assert.fail("testEquals didn't throw an error for class " + clazz.getName());
    } catch (AssertionFailedError | NullPointerException e) {
      // expected
    }
  }
}
