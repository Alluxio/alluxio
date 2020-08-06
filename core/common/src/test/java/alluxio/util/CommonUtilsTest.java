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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.conf.InstancedConfiguration;
import alluxio.security.group.CachedGroupMapping;
import alluxio.security.group.GroupMappingService;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Tests the {@link CommonUtils} class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({CommonUtils.class, ShellUtils.class, GroupMappingService.Factory.class})
public class CommonUtilsTest {

  /**
   * Tests the {@link CommonUtils#convertMsToClockTime(long)} method.
   */
  @Test
  public void convertMsToClockTime() {
    assertEquals("0 day(s), 0 hour(s), 0 minute(s), and 0 second(s)",
        CommonUtils.convertMsToClockTime(10));
    assertEquals("0 day(s), 0 hour(s), 0 minute(s), and 1 second(s)",
        CommonUtils.convertMsToClockTime(TimeUnit.SECONDS.toMillis(1)));
    assertEquals("0 day(s), 0 hour(s), 1 minute(s), and 0 second(s)",
        CommonUtils.convertMsToClockTime(TimeUnit.MINUTES.toMillis(1)));
    assertEquals("0 day(s), 0 hour(s), 1 minute(s), and 30 second(s)",
        CommonUtils.convertMsToClockTime(TimeUnit.MINUTES.toMillis(1)
            + TimeUnit.SECONDS.toMillis(30)));
    assertEquals("0 day(s), 1 hour(s), 0 minute(s), and 0 second(s)",
        CommonUtils.convertMsToClockTime(TimeUnit.HOURS.toMillis(1)));
    long time =
        TimeUnit.DAYS.toMillis(1) + TimeUnit.HOURS.toMillis(4) + TimeUnit.MINUTES.toMillis(10)
            + TimeUnit.SECONDS.toMillis(45);
    String out = CommonUtils.convertMsToClockTime(time);
    assertEquals("1 day(s), 4 hour(s), 10 minute(s), and 45 second(s)", out);
  }

  /**
   * Tests the {@link CommonUtils#getCurrentMs()} and {@link CommonUtils#sleepMs(long)} methods.
   */
  @Test
  public void getCurrentMsAndSleepMs() {
    long delta = 100;
    long startTime = CommonUtils.getCurrentMs();
    CommonUtils.sleepMs(delta);
    long currentTime = CommonUtils.getCurrentMs();

    /* Check that currentTime falls into the interval [startTime + delta; startTime + 2*delta] */
    assertTrue(startTime + delta <= currentTime);
    assertTrue(currentTime <= 2 * delta + startTime);
  }

  @Test
  public void getTmpDir() {

    // Test single tmp dir
    String singleDir = "/tmp";
    List<String> singleDirList = Arrays.asList("/tmp");
    assertEquals(singleDir, CommonUtils.getTmpDir(singleDirList));
    // Test multiple tmp dir
    List<String> multiDirs = Arrays.asList("/tmp1", "/tmp2", "/tmp3");
    Set<String> results = new HashSet<>();
    for (int i = 0; i < 100 || results.size() != multiDirs.size(); i++) {
      results.add(CommonUtils.getTmpDir(multiDirs));
    }
    assertEquals(new HashSet<>(multiDirs), results);
  }

  /**
   * Tests the {@link CommonUtils#argsToString(String, Object[])} method.
   */
  @Test
  public void argsToString() {
    assertEquals("", CommonUtils.argsToString(".", ""));
    assertEquals("foo", CommonUtils.argsToString(".", "foo"));
    assertEquals("foo,bar", CommonUtils.argsToString(",", "foo", "bar"));
    assertEquals("1", CommonUtils.argsToString("", 1));
    assertEquals("1;2;3", CommonUtils.argsToString(";", 1, 2, 3));
  }

  /**
   * Tests the {@link CommonUtils#listToString(List)} method.
   */
  @Test
  public void listToString() {
    class TestCase {
      List<Object> mInput;
      String mExpected;

      public TestCase(String expected, Object... objs) {
        mExpected = expected;
        mInput = Arrays.asList(objs);
      }
    }

    List<TestCase> testCases = new ArrayList<>();
    testCases.add(new TestCase(""));
    testCases.add(new TestCase("foo", "foo"));
    testCases.add(new TestCase("foo bar", "foo", "bar"));
    testCases.add(new TestCase("1", 1));
    testCases.add(new TestCase("1 2 3", 1, 2, 3));

    for (TestCase testCase : testCases) {
      assertEquals(testCase.mExpected, CommonUtils.listToString(testCase.mInput));
    }
  }

  /**
   * Tests the {@link CommonUtils#toStringArray(ArrayList)} method.
   */
  @Test
  public void toStringArray() {
    class TestCase {
      String[] mExpected;

      public TestCase(String... strings) {
        mExpected = strings;
      }
    }

    List<TestCase> testCases = new ArrayList<>();
    testCases.add(new TestCase());
    testCases.add(new TestCase("foo"));
    testCases.add(new TestCase("foo", "bar"));

    for (TestCase testCase : testCases) {
      ArrayList<String> input = new ArrayList<>();
      Collections.addAll(input, testCase.mExpected);
      String[] got = CommonUtils.toStringArray(input);
      assertEquals(testCase.mExpected.length, got.length);
      for (int k = 0; k < got.length; k++) {
        assertEquals(testCase.mExpected[k], got[k]);
      }
    }
  }

  /**
   * Tests the {@link CommonUtils#createNewClassInstance(Class, Class[], Object[])} method.
   */
  @Test
  public void createNewClassInstance() {
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

    List<TestCase> testCases = new ArrayList<>();
    testCases.add(new TestCase("hello", TestClassA.class, null));
    testCases.add(new TestCase("1", TestClassB.class, new Class[] {int.class}, 1));

    for (TestCase testCase : testCases) {
      Object o = CommonUtils.createNewClassInstance(testCase.mCls, testCase.mCtorClassArgs,
          testCase.mCtorArgs);
      assertEquals(o.toString(), testCase.mExpected);
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
  public void userGroup() throws Throwable {
    String userName = "alluxio-user1";
    String userGroup1 = "alluxio-user1-group1";
    String userGroup2 = "alluxio-user1-group2";
    List<String> userGroups = new ArrayList<>();
    userGroups.add(userGroup1);
    userGroups.add(userGroup2);
    setupShellMocks(userName, userGroups);

    List<String> groups = CommonUtils.getUnixGroups(userName);

    assertNotNull(groups);
    assertEquals(groups.size(), 2);
    assertEquals(groups.get(0), userGroup1);
    assertEquals(groups.get(1), userGroup2);
  }

  /**
   * Test for the {@link CommonUtils#getGroups(String)} and
   * {@link CommonUtils#getPrimaryGroupName(String)} method.
   */
  @Test
  public void getGroups() throws Throwable {
    InstancedConfiguration conf = ConfigurationTestUtils.defaults();

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
    Mockito.when(GroupMappingService.Factory.get(conf)).thenReturn(cachedGroupService);

    List<String> groups = CommonUtils.getGroups(userName, conf);
    assertEquals(Arrays.asList(userGroup1, userGroup2), groups);

    String primaryGroup = CommonUtils.getPrimaryGroupName(userName, conf);
    assertNotNull(primaryGroup);
    assertEquals(userGroup1, primaryGroup);
  }

  /**
   * Tests the {@link CommonUtils#stripSuffixIfPresent(String,String)} method.
   */
  @Test
  public void stripSuffixIfPresent() throws Exception {
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
      assertEquals(results[i],
          CommonUtils.stripSuffixIfPresent(inputs[i], suffixToStrip[i]));
    }
  }

  /**
   * Tests the {@link CommonUtils#stripPrefixIfPresent(String, String)} method.
   */
  @Test
  public void stripPrefixIfPresent() throws Exception {
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
      assertEquals(results[i],
          CommonUtils.stripPrefixIfPresent(inputs[i], prefixToStrip[i]));
    }
  }

  @Test
  public void stripLeadingAndTrailingQuotes() throws Exception {
    assertEquals("", CommonUtils.stripLeadingAndTrailingQuotes(""));
    assertEquals("\"", CommonUtils.stripLeadingAndTrailingQuotes("\""));
    assertEquals("", CommonUtils.stripLeadingAndTrailingQuotes("\"\""));
    assertEquals("\"", CommonUtils.stripLeadingAndTrailingQuotes("\"\"\""));
    assertEquals("\"\"", CommonUtils.stripLeadingAndTrailingQuotes("\"\"\"\""));
    assertEquals("noquote", CommonUtils.stripLeadingAndTrailingQuotes("noquote"));
    assertEquals(
        "\"singlequote", CommonUtils.stripLeadingAndTrailingQuotes("\"singlequote"));
    assertEquals(
        "singlequote\"", CommonUtils.stripLeadingAndTrailingQuotes("singlequote\""));
    assertEquals("quoted", CommonUtils.stripLeadingAndTrailingQuotes("\"quoted\""));
    assertEquals("\"quoted\"", CommonUtils.stripLeadingAndTrailingQuotes("\"\"quoted\"\""));
  }

  @Test
  public void getValueFromStaticMapping() throws Exception {
    String mapping = "k=v; a=a; alice=bob; id1=userA; foo=bar";
    assertEquals("v",     CommonUtils.getValueFromStaticMapping(mapping, "k"));
    assertEquals("a",     CommonUtils.getValueFromStaticMapping(mapping, "a"));
    assertEquals("bob",   CommonUtils.getValueFromStaticMapping(mapping, "alice"));
    assertEquals("userA", CommonUtils.getValueFromStaticMapping(mapping, "id1"));
    assertEquals("bar",   CommonUtils.getValueFromStaticMapping(mapping, "foo"));
    assertEquals(null,    CommonUtils.getValueFromStaticMapping(mapping, ""));
    assertEquals(null,    CommonUtils.getValueFromStaticMapping(mapping, "/"));
    assertEquals(null,    CommonUtils.getValueFromStaticMapping(mapping, "v"));
    assertEquals(null,    CommonUtils.getValueFromStaticMapping(mapping, "nonexist"));
  }

  @Test
  public void invokeAllSuccess() throws Exception {
    int numTasks = 5;
    final CyclicBarrier b = new CyclicBarrier(numTasks);
    final AtomicInteger completed = new AtomicInteger();
    List<Callable<Void>> tasks = new ArrayList<>();
    for (int i = 0; i < numTasks; i++) {
      tasks.add(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          b.await();
          completed.incrementAndGet();
          return null;
        }
      });
    }
    CommonUtils.invokeAll(tasks, 10 * Constants.SECOND_MS);
    assertEquals(numTasks, completed.get());
  }

  @Test
  public void invokeAllHang() throws Exception {
    int numTasks = 5;
    List<Callable<Void>> tasks = new ArrayList<>();
    for (int i = 0; i < numTasks; i++) {
      tasks.add(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          Thread.sleep(10 * Constants.SECOND_MS);
          return null;
        }
      });
    }
    try {
      CommonUtils.invokeAll(tasks, 50);
      fail("Expected a timeout exception");
    } catch (TimeoutException e) {
      // Expected
    }
  }

  @Test
  public void invokeAllExceptionAndHang() throws Exception {
    long start = System.currentTimeMillis();
    RuntimeException testException = new RuntimeException("failed");
    try {
      CommonUtils.invokeAll(Arrays.asList(
          () -> {
            Thread.sleep(10 * Constants.SECOND_MS);
            return null;
          },
          () -> {
            throw testException;
          }
      ), 5 * Constants.SECOND_MS);
      fail("Expected an exception to be thrown");
    } catch (ExecutionException e) {
      assertSame(testException, e.getCause());
    }
    assertThat("invokeAll should exit early if one of the tasks throws an exception",
        System.currentTimeMillis() - start, Matchers.lessThan(2L * Constants.SECOND_MS));
  }

  @Test
  public void invokeAllPropagatesException() throws Exception {
    int numTasks = 5;
    final AtomicInteger id = new AtomicInteger();
    List<Callable<Void>> tasks = new ArrayList<>();
    final Exception testException = new Exception("test message");
    for (int i = 0; i < numTasks; i++) {
      tasks.add(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          int myId = id.incrementAndGet();
          // The 3rd task throws an exception
          if (myId == 3) {
            throw testException;
          }
          return null;
        }
      });
    }
    try {
      CommonUtils.invokeAll(tasks, 2 * Constants.SECOND_MS);
      fail("Expected an exception to be thrown");
    } catch (ExecutionException e) {
      assertSame(testException, e.getCause());
    }
  }

  /**
   * Tests that when one task throws an exception and other tasks time out, the exception is
   * propagated.
   */
  @Test
  public void invokeAllPropagatesExceptionWithTimeout() throws Exception {
    int numTasks = 5;
    final AtomicInteger id = new AtomicInteger();
    List<Callable<Void>> tasks = new ArrayList<>();
    final Exception testException = new Exception("test message");
    for (int i = 0; i < numTasks; i++) {
      tasks.add(() -> {
        int myId = id.incrementAndGet();
        // The 3rd task throws an exception, other tasks sleep.
        if (myId == 3) {
          throw testException;
        } else {
          Thread.sleep(10 * Constants.SECOND_MS);
        }
        return null;
      });
    }
    try {
      CommonUtils.invokeAll(tasks, 500);
      fail("Expected an exception to be thrown");
    } catch (ExecutionException e) {
      assertSame(testException, e.getCause());
    }
  }

  /** Returns true starting at the nth query. */
  private static class CountCondition implements Supplier<Boolean> {
    private final int mTarget;
    private int mCount = 0;

    public CountCondition(int target) {
      mTarget = target;
    }

    @Override
    public Boolean get() {
      return ++mCount >= mTarget;
    }

    private int invocations() {
      return mCount;
    }
  }

  @Test
  public void waitForFirstTry() throws Exception {
    testNthSuccess(1);
  }

  @Test
  public void waitForSecondTry() throws Exception {
    testNthSuccess(2);
  }

  @Test
  public void waitForFiftyTry() throws Exception {
    testNthSuccess(5);
  }

  private void testNthSuccess(int n) throws Exception {
    CountCondition cond = new CountCondition(n);
    int intervalMs = 10;
    WaitForOptions opts = WaitForOptions.defaults().setInterval(intervalMs);
    long start = System.currentTimeMillis();
    CommonUtils.waitFor("", cond, opts);
    long durationMs = System.currentTimeMillis() - start;
    assertThat((int) durationMs, Matchers.greaterThanOrEqualTo((n - 1) * intervalMs));
    assertEquals(n, cond.invocations());
  }

  @Test(expected = TimeoutException.class)
  public void waitForTimeout() throws Exception {
    CountCondition cond = new CountCondition(100);
    WaitForOptions opts = WaitForOptions.defaults().setInterval(3).setTimeoutMs(100);
    CommonUtils.waitFor("", cond, opts);
  }

  @Test(timeout = 10000)
  public void interruptInvokeAll() throws Exception {
    // this should be significantly longer than the test timeout (10s)
    long longWaitMs = 1000000;
    List<Callable<Void>> tasks = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      tasks.add(() -> {
        CommonUtils.sleepMs(longWaitMs);
        return null;
      });
    }

    Thread waiting = new Thread(() -> {
      try {
        CommonUtils.invokeAll(tasks, longWaitMs);
      } catch (RuntimeException e) {
        // expected, since it is interrupted
      } catch (Exception e) {
        fail("invokeAll threw unexpected exception: " + e);
      }
    });
    try {
      waiting.start();
      waiting.interrupt();
    } finally {
      waiting.join();
    }
  }

  @Test(timeout = 10000)
  public void invokeAllTimeoutCleanup() throws Exception {
    // this should be significantly longer than the test timeout (10s)
    long longWaitMs = 1000000;
    ThreadPoolExecutor service = (ThreadPoolExecutor) Executors.newCachedThreadPool();

    try {
      List<Callable<Void>> tasks = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        tasks.add(() -> {
          CommonUtils.sleepMs(longWaitMs);
          return null;
        });
      }

      try {
        CommonUtils.invokeAll(service, tasks, 10);
        fail("invokeAll is expected to timeout, not succeed");
      } catch (TimeoutException e) {
        // expected
      }

      CommonUtils.waitFor("all threads to stop after timeout", () -> service.getActiveCount() == 0,
          WaitForOptions.defaults().setInterval(10).setTimeoutMs(1000));
    } finally {
      service.shutdownNow();
    }
  }

  @Test
  public void recursiveList() throws Exception {
    File tmpDirFile = Files.createTempDir();
    tmpDirFile.deleteOnExit();

    Set<File> allFiles = new HashSet<>();
    // Create 10 files at randomly deep level in the directory
    for (int i = 0; i < 10; i++) {
      createFileOrDir(tmpDirFile, i, new Random(), allFiles);
    }

    List<File> listedFiles = CommonUtils.recursiveListLocalDir(tmpDirFile);
    assertEquals(allFiles, new HashSet<>(listedFiles));
  }

  @Test
  public void parseVersion() throws Exception {
    assertEquals(8, CommonUtils.parseMajorVersion("1.8.0"));
    assertEquals(11, CommonUtils.parseMajorVersion("11.0.1"));
    assertEquals(9, CommonUtils.parseMajorVersion("9.0.1"));
  }

  private void createFileOrDir(File dir, int index, Random rand, Set<File> files)
          throws IOException {
    int childType = rand.nextInt(2);
    File child = new File(dir, index + "");
    if (childType == 1) {
      // The child is a directory, go deeper into it
      child.mkdir();
      createFileOrDir(child, index, rand, files);
    } else {
      // The child is a file
      child.createNewFile();
      files.add(child);
    }
  }
}
