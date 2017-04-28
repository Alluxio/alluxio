---
layout: global
title: Unit Testing Guidelines
nickname: Unit Testing Guidelines
group: Resources
---

* Table of Contents
{:toc}

## Unit testing goals

1. Unit tests act as examples of how to use the code under test.
2. Unit tests detect when an object breaks it's specification.
3. Unit tests *don't* break when an object is refactored but still meets the same specification.

## How to write a unit test

1. If creating an instance of the class takes some work, create a `@Before` method to perform common setup. The `@Before` method gets run automatically before each unit test. Only do general setup which will apply to every test. Test-specific setup should be done locally in the tests that need it. In this example, we are testing a `BlockMaster`, which depends on a journal, clock, and executor service. The executor service and journal we provide are real implementations, and the `TestClock` is a fake clock which can be controlled by unit tests.

```java
@Before
public void before() throws Exception {
  Journal blockJournal = new ReadWriteJournal(mTestFolder.newFolder().getAbsolutePath());
  mClock = new TestClock();
  mExecutorService =
      Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("TestBlockMaster-%d", true));
  mMaster = new BlockMaster(blockJournal, mClock, mExecutorService);
  mMaster.start(true);
}
```

2. If anything created in `@Before` creates something which needs to be cleaned up (e.g. a `BlockMaster`), create an `@After` method to do the cleanup. This method is automatically called after each test.

```java
@After
public void after() throws Exception {
  mMaster.stop();
}
```

3. Decide on an element of functionality to test. The functionality you decide to test should be part of the public API and should not care about implementation details. Tests should be focused on testing only one thing.

4. Give your test a name that describes what functionality it's testing. The functionality being tested should ideally be simple enough to fit into a name, e.g. `removeNonexistentBlockThrowsException`, `mkdirCreatesDirectory`, or `cannotMkdirExistingFile`.

```java
@Test
public void detectLostWorker() throws Exception {
```
5. Set up the situation you want to test. Here we register a worker and then simulate an hour passing. The `HeartbeatScheduler` section enforces that the lost worker heartbeat runs at least once.

```java
  // Register a worker.
  long worker1 = mMaster.getWorkerId(NET_ADDRESS_1);
  mMaster.workerRegister(worker1,
      ImmutableList.of("MEM"),
      ImmutableMap.of("MEM", 100L),
      ImmutableMap.of("MEM", 10L),
      NO_BLOCKS_ON_TIERS);

  // Advance the block master's clock by an hour so that the worker appears lost.
  mClock.setTimeMs(System.currentTimeMillis() + Constants.HOUR_MS);

  // Run the lost worker detector.
  HeartbeatScheduler.await(HeartbeatContext.MASTER_LOST_WORKER_DETECTION, 1, TimeUnit.SECONDS);
  HeartbeatScheduler.schedule(HeartbeatContext.MASTER_LOST_WORKER_DETECTION);
  HeartbeatScheduler.await(HeartbeatContext.MASTER_LOST_WORKER_DETECTION, 1, TimeUnit.SECONDS);
```
6. Check that the class behaved correctly:

```java
  // Make sure the worker is detected as lost.
  Set<WorkerInfo> info = mMaster.getLostWorkersInfo();
  Assert.assertEquals(worker1, Iterables.getOnlyElement(info).getId());
}
```
7. Loop back to step #3 until the class's entire public API has been tested.

# Conventions
1. The tests for `src/main/java/ClassName.java` should go in `src/test/java/ClassNameTest.java`
2. Tests do not need to handle or document specific checked exceptions. Prefer to simply add `throws Exception` to the test method signature.
3. Aim to keep tests short and simple enough that they don't require comments to understand.

# Patterns to avoid

1. Avoid randomness. Edge cases should be handled explicitly.
2. Avoid waiting for something by calling `Thread.sleep()`. This leads to slower unit tests and can cause flaky failures if the sleep isn't long enough.
3. Avoid using Whitebox to mess with the internal state of objects under test. If you need to mock a dependency, change the object to take the dependency as a parameter in its constructor (see [dependency injection](https://en.wikipedia.org/wiki/Dependency_injection))
4. Avoid slow tests. Mock expensive dependencies and aim to keep individual test times under 100ms.

# Managing Global State
All tests in a module run in the same JVM, so it's important to properly manage global state so that tests don't interfere with each other. Global state includes system properties, Alluxio configuration, and any static fields. Our solution to managing global state is to use JUnit's support for `@Rules`.

### Changing Alluxio configuration during tests

Some unit tests want to test Alluxio under different configurations. This requires modifying the global `Configuration` object. When all tests in a suite need configuration parameters set a certain way, use `ConfigurationRule` to set them.

```java
@Rule
public ConfigurationRule mConfigurationRule = new ConfigurationRule(ImmutableMap.of(
    PropertyKey.key1, "value1",
    PropertyKey.key2, "value2"));
```
For configuration changes needed for an individual test, use `Configuration.set(key, value)`, and create an `@After` method to clean up the configuration change after the test:

```java
@After
public void after() {
  ConfigurationTestUtils.resetConfiguration();
}

@Test
public void testSomething() {
  Configuration.set(PropertyKey.key, "value");
  ...
}
```

### Changing System properties during tests

If you need to change a system property for the duration of a test suite, use `SystemPropertyRule`.

```java
@Rule
public SystemPropertyRule mSystemPropertyRule = new SystemPropertyRule("propertyName", "value");
```

To set a system property during a specific test, use `SetAndRestoreSystemProperty` in a try-catch statement:

```java
@Test
public void test() {
  try (SetAndRestorySystemProperty p = new SetAndRestorySystemProperty("propertyKey", "propertyValue")) {
     // Test something with propertyKey set to propertyValue.
  }
}
```

### Other global state

If a test needs to modify other types of global state, create a new `@Rule` for managing the state so that it can be shared across tests. One example of this is [`TtlIntervalRule`](https://github.com/Alluxio/alluxio/blob/master/core/server/master/src/test/java/alluxio/master/file/meta/TtlIntervalRule.java).
