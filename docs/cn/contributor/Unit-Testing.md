---
layout: global
title: 如何开发单元测试
nickname: 如何开发单元测试
group: Contributor Resources
priority: 3
---

* 内容列表
{:toc}

## 单元测试目标
1. 单元测试用于示例如何使用测试中的代码。
2. 单元测试在对象失去功能时检测。
3. 当对象被重构但仍满足相同功能时，单元测试不会打断它。

## 如何编写单元测试

1\. 如果创建类的实例需要一些工作，创建一个`@Before`方法来执行常用的设置。该`@Before`方法在每个单元测试之前被自动运行。只有适用于所有测试才做通用设置。特定的测试设置应该在本地需要它的测试中完成。在这个例子中，我们测试`BlockMaster`，它依赖于一个日志，时钟和executor service。我们提供的executor service和日志是真正的实现，`TestClock`是可以由单元测试来控制的伪时钟。

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

2\. 如果任何在`@Before`中创建的东西需要进行清理（例如一个`BlockMaster`），创建一个`@After`方法来做清理。此方法在每次测试后被自动调用。

```java
@After
public void after() throws Exception {
  mMaster.stop();
}
```

3\. 确定一个功能性元素来测试。你决定要测试的功能应该是公共API的一部分，而不应该与实现的细节相关。测试应专注于只测试一项功能。

4\. 给你的测试起一个名字，来描述它测试的功能。被测试的功能最好足够简单，以便用一个名字就能表示，例如`removeNonexistentBlockThrowsException`, `mkdirCreatesDirectory`, 或者`cannotMkdirExistingFile`。

```java
@Test
public void detectLostWorker() throws Exception {
```
5\. 设置要测试的场景。这里我们注册一个worker，然后模拟一小时。`HeartbeatScheduler`部分确保丢失worker的heartbeat至少执行一次。

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
6\. 检查类的执行是否正确：

```java
  // Make sure the worker is detected as lost.
  Set<WorkerInfo> info = mMaster.getLostWorkersInfo();
  Assert.assertEquals(worker1, Iterables.getOnlyElement(info).getId());
}
```
7\. 循环回到步骤＃3，直到类的整个公共API都已经被测试。

## 惯例
1. 对`src/main/java/ClassName.java`的测试应该运行为`src/test/java/ClassNameTest.java`。
2. 测试不需要处理或记录特定的检查异常，更倾向于简单地添加`throws Exception`到方法声明中。
3. 目标是保持测试简明扼要而不需要注释帮助理解。

## 应避免的情况

1. 避免随机性。边缘检测应被明确处理。
2. 避免通过调用`Thread.sleep()`来等待。这会导致单元测试变慢，如果时间不够长，可能导致时间片的失败。
3. 避免使用白盒测试，这会混乱测试对象的内部状态。如果你需要模拟一个依赖，改变对象，将依赖作为其构造函数的参数（见[依赖注入](https://en.wikipedia.org/wiki/Dependency_injection)）
4. 避免低效测试。模拟花销较大的依赖关系，将各个测试时间控制在100ms以下。

## 管理全局状态
一个模块中的所有测试在同一个JVM上运行，所以妥善管理全局状态是非常重要的，这样测试不会互相干扰。全局状态包括系统性能，Alluxio配置，以及任何静态字段。我们对管理全局状态的解决方案是使用JUnit对`@Rules`的支持。

### 在测试过程中更改Alluxio配置
一些单元测试需要测试不同配置下的Alluxio。这需要修改全局`Configuration`对象。当在一个套件中的所有测试都需要配置参数时，设置某一个方式，用`ConfigurationRule`对它们进行设置。

```java
@Rule
public ConfigurationRule mConfigurationRule = new ConfigurationRule(ImmutableMap.of(
    PropertyKey.key1, "value1",
    PropertyKey.key2, "value2"));
```
对于一个单独测试需要的配置更改，请使用`Configuration.set(key, value)`，并创建一个`@After`方法来清理测试后的配置更改：

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

### 在测试过程中更改系统属性
如果你在测试套件期间需要更改一个系统属性，请使用`SystemPropertyRule`。

```java
@Rule
public SystemPropertyRule mSystemPropertyRule = new SystemPropertyRule("propertyName", "value");
```

在一个特定的测试中设置系统属性，请在try-catch语句中使用`SetAndRestoreSystemProperty`：

```java
@Test
public void test() {
  try (SetAndRestorySystemProperty p = new SetAndRestorySystemProperty("propertyKey", "propertyValue")) {
     // Test something with propertyKey set to propertyValue.
  }
}
```

### 其他全局状态
如果测试需要修改其它类型的全局状态，创建一个新的`@Rule`用于管理状态，这样就可以在测试中共享。这样的一个例子是[`TtlIntervalRule`](https://github.com/Alluxio/alluxio/blob/master/core/server/master/src/test/java/alluxio/master/file/meta/TtlIntervalRule.java)。
