---
layout: global
title: Code Conventions
nickname: Code Conventions
group: Contributor Resources
priority: 2
---

* Table of Contents
{:toc}

First off, we thank you for your interest in the Alluxio open source project!
We greatly appreciate any contribution; whether new features or bug fixes.

> If you are a first time contributor to the Alluxio open source project, we strongly encourage
> you to follow the step-by-step instructions within the
> [Contribution Guide]({{ '/en/contributor/Contributor-Getting-Started.html' | relativize_url }}) and
> finish a new contributor task before making more advanced changes to Alluxio.

## Submitting Code

- We encourage you to break your work into small, single-purpose patches if possible. It is much
harder to merge in a large change with a lot of disjoint features.
- We track issues and features in our [JIRA](https://alluxio.atlassian.net/). If you have not
registered an account, please do so!
- Open a ticket in [JIRA](https://alluxio.atlassian.net/) detailing the proposed change and what
purpose it serves.
- Submit the patch as a GitHub pull request. For a tutorial, see the GitHub guides on
[forking a repo](https://help.github.com/articles/fork-a-repo) and
[sending a pull request](https://help.github.com/articles/using-pull-requests).
- In your pull request title, make sure to reference the JIRA ticket. This will connect the
ticket to the proposed code changes. for example:

~~~~~
[ALLUXIO-100] Implement an awesome new feature
~~~~~

- In the description field of the pull request, please include a link to the JIRA ticket.

Note that for some minor changes it is not required to create corresponding JIRA tickets before
submitting the pull requests. For instance:

- For pull requests that only address typos or formatting issues in source code, you
can prefix the titles of your pull requests with "[SMALLFIX]", for example:

~~~~~
[SMALLFIX] Fix formatting in Foo.java
~~~~~

- For pull requests that improve the documentation of Alluxio project website (e.g., modify the
markdown files in `docs` directory), you can prefix the titles of your pull requests with
"[DOCFIX]". For example, to edit this web page which is generated from
`docs/Contributing-to-Alluxio.md`, the title can be:

~~~~~
[DOCFIX] Improve documentation of how to contribute to Alluxio
~~~~~



### System Settings

Sometimes you will need to play with a few system settings in order to have the unit tests pass
locally. A common setting that may need to be set is `ulimit`.

In order to increase the number of files and processes allowed on MacOS, run the following

```bash
$ sudo launchctl limit maxfiles 32768 32768
$ sudo launchctl limit maxproc 32768 32768
```

It is also recommended to exclude your local clone of Alluxio from Spotlight indexing. Otherwise,
your Mac may hang constantly trying to re-index the file system during the unit tests. To do this,
go to `System Preferences > Spotlight > Privacy`, click the `+` button, browse to the directory
containing your local clone of Alluxio, and click `Choose` to add it to the exclusions list.


## Coding Style

- Please follow the style of the existing codebase. Specifically, we use
[Google Java style](https://google.github.io/styleguide/javaguide.html),
with the following changes or deviations:
  - Maximum line length of **100** characters.
  - Third-party imports are grouped together to make IDE formatting much simpler.
  - Class member variable names should be prefixed with `m`, for example `private WorkerClient
    mWorkerClient;`
  - Static variable names should be prefixed with `s`, for example `public static String
    sUnderFSAddress;`
- Bash scripts follow [Google Shell style](https://google.github.io/styleguide/shell.xml), and
must be compatible with Bash 3.x
- You can download our
[Eclipse formatter]({{ '/resources/alluxio-code-formatter-eclipse.xml' | relativize_url }})
- For Eclipse to organize your imports correctly, configure "Organize Imports" to look like
[this]({{ '/resources/eclipse_imports.png' | relativize_url }})
- If you use IntelliJ IDEA:
- You can either use our formatter with the help from
[Eclipse Code Formatter](https://github.com/krasa/EclipseCodeFormatter#instructions)
or use [Eclipse Code Formatter Plugin](http://plugins.jetbrains.com/plugin/6546) in
IntelliJ IDEA.
- To automatically format the **import**, configure in
Preferences->Code Style->Java->Imports->Import Layout according to
[this order]({{ '/resources/intellij_imports.png' | relativize_url }})
- To automatically reorder methods alphabetically, try the
[Rearranger Plugin](http://plugins.jetbrains.com/plugin/173), open Preferences, search for
rearranger, remove the unnecessary comments, then right click, choose "Rearrange", codes
will be formatted to what you want

- To verify that the coding standards match, you should run
[checkstyle](http://checkstyle.sourceforge.net) before sending a pull-request to verify no new
warnings are introduced:

```bash
$ mvn checkstyle:checkstyle
```

## Logging Conventions

Alluxio uses [SLF4J](https://www.slf4j.org/) for logging with typical usage pattern of:

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public MyClass {

  private static final Logger LOG = LoggerFactory.getLogger(MyClass.class);

  public void someMethod() {
    LOG.info("Hello world");
  }
}
```

Here is the convention in Alluxio source code on different log levels:

* Error level logging (i.e., `LOG.error`) indicates system level problems which cannot be recovered from.
It should always be accompanied by a stack trace.
```java
LOG.error("Failed to do something due to an exception", e);
```
* Warn level logging (i.e., `LOG.warn`) indicates a logical mismatch between user intended behavior
and Alluxio behavior. Warn level logs are accompanied by an exception message. The associated stack
trace may be found in debug level logs.
```java
LOG.warn("Failed to do something due to {}", e.getMessage());
```
* Info level logging (i.e., `LOG.info`) records important system state changes. Exception messages
and stack traces are never associated with info level logs. Note that, this level of logging should
not be used on critical path of operations that may happen frequently to prevent negative performance
impact.
```java
LOG.info("Master started.");
```

* Debug level logging (i.e., `LOG.debug`) includes detailed information for various aspects of
the Alluxio system. Control flow logging (Alluxio system enter and exit calls) is done in debug
level logs. Debug level logging of exceptions typically has the detailed information including
stack trace. Please avoid the slow strings construction on debug-level logging on critical path.
```java
LOG.debug("Failed to connect to {} due to exception", host + ":" + port, e); // wrong
LOG.debug("Failed to connect to {} due to exception", mAddress, e); // OK
if (LOG.isDebugEnabled()) {
    LOG.debug("Failed to connect to address {} due to exception", host + ":" + port, e); // OK
}
```
* Trace level logging (i.e., `LOG.trace`) is not used in Alluxio.

## Unit Testing

### Unit Test Goals

1. Unit tests act as examples of how to use the code under test.
2. Unit tests detect when an object breaks it's specification.
3. Unit tests *don't* break when an object is refactored but still meets the same specification.

### How to Write a Unit Test

1. If creating an instance of the class takes some work, create a `@Before` method to perform
common setup. The `@Before` method gets run automatically before each unit test. Only do general
setup which will apply to every test. Test-specific setup should be done locally in the tests that
need it. In this example, we are testing a `BlockMaster`, which depends on a journal, clock, and
executor service. The executor service and journal we provide are real implementations, and the
`TestClock` is a fake clock which can be controlled by unit tests.

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

2. If anything created in `@Before` creates something which needs to be cleaned up (e.g. a
`BlockMaster`), create an `@After` method to do the cleanup. This method is automatically called
after each test.

```java
@After
public void after() throws Exception {
  mMaster.stop();
}
```

3. Decide on an element of functionality to test. The functionality you decide to test should be
part of the public API and should not care about implementation details. Tests should be focused
on testing only one thing.

4. Give your test a name that describes what functionality it's testing. The functionality being
tested should ideally be simple enough to fit into a name, e.g.
`removeNonexistentBlockThrowsException`, `mkdirCreatesDirectory`, or `cannotMkdirExistingFile`.

```java
@Test
  public void detectLostWorker() throws Exception {
```
5. Set up the situation you want to test. Here we register a worker and then simulate an hour
passing. The `HeartbeatScheduler` section enforces that the lost worker heartbeat runs at least
once.

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

### Conventions

1. The tests for `src/main/java/ClassName.java` should go in `src/test/java/ClassNameTest.java`
2. Tests do not need to handle or document specific checked exceptions. Prefer to simply add
`throws Exception` to the test method signature.
3. Aim to keep tests short and simple enough that they don't require comments to understand.

### Patterns to avoid

1. Avoid randomness. Edge cases should be handled explicitly.
2. Avoid waiting for something by calling `Thread.sleep()`. This leads to slower unit tests and can
cause flaky failures if the sleep isn't long enough.
3. Avoid using Whitebox to mess with the internal state of objects under test. If you need to mock
a dependency, change the object to take the dependency as a parameter in its constructor
(see [dependency injection](https://en.wikipedia.org/wiki/Dependency_injection))
4. Avoid slow tests. Mock expensive dependencies and aim to keep individual test times under 100ms.

### Managing Global State
All tests in a module run in the same JVM, so it's important to properly manage global state so
that tests don't interfere with each other. Global state includes system properties, Alluxio
configuration, and any static fields. Our solution to managing global state is to use JUnit's
support for `@Rules`.

#### Changing Alluxio configuration during tests

Some unit tests want to test Alluxio under different configurations. This requires modifying the
global `Configuration` object. When all tests in a suite need configuration parameters set a
certain way, use `ConfigurationRule` to set them.

```java
@Rule
public ConfigurationRule mConfigurationRule = new ConfigurationRule(ImmutableMap.of(
    PropertyKey.key1, "value1",
    PropertyKey.key2, "value2"));
```
For configuration changes needed for an individual test, use `Configuration.set(key, value)`, and
create an `@After` method to clean up the configuration changes after the test:

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

#### Changing System properties during tests

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

#### Other global state

If a test needs to modify other types of global state, create a new `@Rule` for managing the state so that it can be shared across tests. One example of this is [`TtlIntervalRule`](https://github.com/Alluxio/alluxio/blob/master/core/server/master/src/test/java/alluxio/master/file/meta/TtlIntervalRule.java).
