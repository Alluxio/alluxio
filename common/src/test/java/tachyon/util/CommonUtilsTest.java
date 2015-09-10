/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.util;

import java.lang.Thread;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class CommonUtilsTest {

  @Test
  public void getCurrentMsAndSleepMsTest() {
    long delta = 100;
    long startTime = CommonUtils.getCurrentMs();
    CommonUtils.sleepMs(delta);
    long currentTime = CommonUtils.getCurrentMs();

    /* Check that currentTime falls into the interval [startTime + delta; startTime + 2*delta] */
    Assert.assertTrue(startTime + delta <= currentTime);
    Assert.assertTrue(currentTime <= 2 * delta + startTime);
  }

  @Test
  public void listToStringTest() {
    class TestCase {
      List<Object> mInput;
      String mExpected;

      public TestCase(String expected, Object... objs) {
        mExpected = expected;
        mInput = Arrays.asList(objs);
      }
    }

    List<TestCase> testCases = new LinkedList<TestCase>();
    testCases.add(new TestCase(""));
    testCases.add(new TestCase("foo", "foo"));
    testCases.add(new TestCase("foo bar", "foo", "bar"));
    testCases.add(new TestCase("1", 1));
    testCases.add(new TestCase("1 2 3", 1, 2, 3));

    for (TestCase testCase : testCases) {
      Assert.assertEquals(testCase.mExpected, CommonUtils.listToString(testCase.mInput));
    }
  }

  @Test
  public void toStringArrayTest() {
    class TestCase {
      String[] mExpected;

      public TestCase(String... strings) {
        mExpected = strings;
      }
    }

    List<TestCase> testCases = new LinkedList<TestCase>();
    testCases.add(new TestCase());
    testCases.add(new TestCase("foo"));
    testCases.add(new TestCase("foo", "bar"));

    for (TestCase testCase : testCases) {
      ArrayList<String> input = new ArrayList<String>();
      for (String s : testCase.mExpected) {
        input.add(s);
      }
      String[] got = CommonUtils.toStringArray(input);
      Assert.assertEquals(testCase.mExpected.length, got.length);
      for (int k = 0; k < got.length; k ++) {
        Assert.assertEquals(testCase.mExpected[k], got[k]);
      }
    }
  }

  @Test
  public void createNewClassInstanceTest() {
    class TestCase {
      Class<?> mCls;
      Class<?>[] mCtorClassArgs;
      Object[] mCtorArgs;
      String mExpected;

      public TestCase(String expected, Class<?> cls, Class<?>[] ctorClassArgs, Object... ctorArgs) {
        mCls = cls;
        mCtorClassArgs = ctorClassArgs;
        mCtorArgs = ctorArgs;
        mExpected = expected;
      }
    }

    List<TestCase> testCases = new LinkedList<TestCase>();
    testCases.add(new TestCase("hello", TestClassA.class, null));
    testCases.add(new TestCase("1", TestClassB.class, new Class[] {int.class}, 1));

    for (TestCase testCase : testCases) {
      try {
        Object o =
            CommonUtils.createNewClassInstance(testCase.mCls, testCase.mCtorClassArgs,
                testCase.mCtorArgs);
        Assert.assertEquals(o.toString(), testCase.mExpected);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  static class TestClassA {
    public TestClassA() {}

    public String toString() {
      return "hello";
    }
  }

  static class TestClassB {
    int mX;

    public TestClassB(int x) {
      mX = x;
    }

    public String toString() {
      return Integer.toString(mX);
    }
  }
}
