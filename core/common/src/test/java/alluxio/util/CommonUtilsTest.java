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

package alluxio.util;

import alluxio.security.group.CachedGroupMapping;
import alluxio.security.group.GroupMappingService;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Tests the {@link CommonUtils} class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ShellUtils.class, GroupMappingService.Factory.class})
public class CommonUtilsTest {

  /**
   * Tests the {@link CommonUtils#getCurrentMs()} and {@link CommonUtils#sleepMs(long)} methods.
   */
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

  /**
   * Tests the {@link CommonUtils#argsToString(String, Object[])} method.
   */
  @Test
  public void argsToStringTest() {
    Assert.assertEquals("", CommonUtils.argsToString(".", ""));
    Assert.assertEquals("foo", CommonUtils.argsToString(".", "foo"));
    Assert.assertEquals("foo,bar", CommonUtils.argsToString(",", "foo", "bar"));
    Assert.assertEquals("1", CommonUtils.argsToString("", 1));
    Assert.assertEquals("1;2;3", CommonUtils.argsToString(";", 1, 2, 3));
  }

  /**
   * Tests the {@link CommonUtils#listToString(List)} method.
   */
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

    List<TestCase> testCases = new LinkedList<>();
    testCases.add(new TestCase(""));
    testCases.add(new TestCase("foo", "foo"));
    testCases.add(new TestCase("foo bar", "foo", "bar"));
    testCases.add(new TestCase("1", 1));
    testCases.add(new TestCase("1 2 3", 1, 2, 3));

    for (TestCase testCase : testCases) {
      Assert.assertEquals(testCase.mExpected, CommonUtils.listToString(testCase.mInput));
    }
  }

  /**
   * Tests the {@link CommonUtils#toStringArray(ArrayList)} method.
   */
  @Test
  public void toStringArrayTest() {
    class TestCase {
      String[] mExpected;

      public TestCase(String... strings) {
        mExpected = strings;
      }
    }

    List<TestCase> testCases = new LinkedList<>();
    testCases.add(new TestCase());
    testCases.add(new TestCase("foo"));
    testCases.add(new TestCase("foo", "bar"));

    for (TestCase testCase : testCases) {
      ArrayList<String> input = new ArrayList<>();
      Collections.addAll(input, testCase.mExpected);
      String[] got = CommonUtils.toStringArray(input);
      Assert.assertEquals(testCase.mExpected.length, got.length);
      for (int k = 0; k < got.length; k++) {
        Assert.assertEquals(testCase.mExpected[k], got[k]);
      }
    }
  }

  /**
   * Tests the {@link CommonUtils#createNewClassInstance(Class, Class[], Object[])} method.
   */
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

    List<TestCase> testCases = new LinkedList<>();
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

    @Override
    public String toString() {
      return "hello";
    }
  }

  static class TestClassB {
    int mX;

    public TestClassB(int x) {
      mX = x;
    }

    @Override
    public String toString() {
      return Integer.toString(mX);
    }
  }

  private void setupShellMocks(String username, List<String> groups) throws IOException {
    PowerMockito.mockStatic(ShellUtils.class);
    String shellResult = "";
    for (String group: groups) {
      shellResult = shellResult + " " + group;
    }
    PowerMockito.when(
        ShellUtils.execCommand(ShellUtils.getGroupsForUserCommand(Mockito.eq(username))))
        .thenReturn(shellResult);
  }

  /**
   * Tests the {@link CommonUtils#getUnixGroups(String)} method.
   */
  @Test
  public void userGroupTest() throws Throwable {
    String userName = "alluxio-user1";
    String userGroup1 = "alluxio-user1-group1";
    String userGroup2 = "alluxio-user1-group2";
    List<String> userGroups = new ArrayList<>();
    userGroups.add(userGroup1);
    userGroups.add(userGroup2);
    setupShellMocks(userName, userGroups);

    List<String> groups = CommonUtils.getUnixGroups(userName);

    Assert.assertNotNull(groups);
    Assert.assertEquals(groups.size(), 2);
    Assert.assertEquals(groups.get(0), userGroup1);
    Assert.assertEquals(groups.get(1), userGroup2);
  }

  /**
   * Test for the {@link CommonUtils#getGroups(String)} and
   * {@link CommonUtils#getPrimaryGroupName(String)} method.
   */
  @Test
  public void getGroupsTest() throws Throwable {
    String userName = "alluxio-user1";
    String userGroup1 = "alluxio-user1-group1";
    String userGroup2 = "alluxio-user1-group2";
    List<String> userGroups = new ArrayList<>();
    userGroups.add(userGroup1);
    userGroups.add(userGroup2);
    CachedGroupMapping cachedGroupService = PowerMockito.mock(CachedGroupMapping.class);
    PowerMockito.when(cachedGroupService.getGroups(Mockito.anyString())).thenReturn(
        Lists.newArrayList(userGroup1, userGroup2));
    PowerMockito.mockStatic(GroupMappingService.Factory.class);
    Mockito.when(GroupMappingService.Factory.getCachedGroupMapping())
        .thenReturn(cachedGroupService);

    List<String> groups = CommonUtils.getGroups(userName);
    Assert.assertEquals(Arrays.asList(userGroup1, userGroup2), groups);

    String primaryGroup = CommonUtils.getPrimaryGroupName(userName);
    Assert.assertNotNull(primaryGroup);
    Assert.assertEquals(userGroup1, primaryGroup);
  }

  /**
   * Tests the {@link CommonUtils#stripSuffixIfPresent(String,String)} method.
   */
  @Test
  public void stripSuffixIfPresentTest() throws Exception {
    final String[] inputs = new String[]{
        "ufs://bucket/",
        "ufs://bucket/",
        "ufs://bucket/file_SUCCESS",
        "ufs://bucket-2/dir/file/",
        "dir/file",
        "/dir/file/",
        "ufs://bucket/file_$folder"
    };
    final String[] suffixToStrip = new String[]{
        "ufs://bucket/",
        "/",
        "_SUCCESS",
        "/",
        "/",
        "/",
        "_$folder"
    };
    final String[] results = new String[]{
        "",
        "ufs://bucket",
        "ufs://bucket/file",
        "ufs://bucket-2/dir/file",
        "dir/file",
        "/dir/file",
        "ufs://bucket/file"
    };
    for (int i = 0; i < inputs.length; i++) {
      Assert.assertEquals(results[i],
          CommonUtils.stripSuffixIfPresent(inputs[i], suffixToStrip[i]));
    }
  }

  /**
   * Tests the {@link CommonUtils#stripPrefixIfPresent(String, String)} method.
   */
  @Test
  public void stripPrefixIfPresentTest() throws Exception {
    final String[] inputs = new String[]{
        "ufs://bucket/",
        "ufs://bucket",
        "ufs://bucket/",
        "ufs://bucket-2/dir/file",
        "dir/file",
        "/dir/file",
        "ufs://bucket/file"
    };
    final String[] prefixToStrip = new String[]{
        "ufs://bucket/",
        "ufs://bucket/",
        "",
        "ufs://bucket-2/",
        "ufs://bucket",
        "/",
        "ufs://bucket/"
    };
    final String[] results = new String[]{
        "",
        "ufs://bucket",
        "ufs://bucket/",
        "dir/file",
        "dir/file",
        "dir/file",
        "file"
    };
    for (int i = 0; i < inputs.length; i++) {
      Assert.assertEquals(results[i],
          CommonUtils.stripPrefixIfPresent(inputs[i], prefixToStrip[i]));
    }
  }

}
