---
layout: global
title: Unit Testing Guidelines
nickname: Unit Testing Guidelines
group: Resources
---

* Table of Contents
{:toc}

# Unit testing goals
1. Unit tests act as examples of how to use the code under test.
2. Unit tests detect when an object breaks it's specification.
3. Unit tests *don't* break when an object is refactored but still meets the same specification.

# How to write a unit test

1. If creating an instance of the class takes some work, create `@Before` and `@After` methods which handle setup and cleanup. These methods get run automatically before and after each unit test. Only do general setup which will apply to every test. Test-specific setup should be done locally in the tests that need it. In this example, we are testing a `BlockMaster`, which depends on a journal, clock, and executor service. The executor service and journal we provide are real implementations, and the `TestClock` is a fake clock which can be controlled by unit tests.

	    @Before
	    public void before() throws Exception {
	      Journal blockJournal = new ReadWriteJournal(mTestFolder.newFolder().getAbsolutePath());
	      mClock = new TestClock();
	      mExecutorService =
	          Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("TestBlockMaster-%d", true));
	      mMaster = new BlockMaster(blockJournal, mClock, mExecutorService);
	      mMaster.start(true);
	    }

	    @After
	    public void after() throws Exception {
	      mMaster.stop();
	    }

2. Decide on an element of functionality to test. The functionality you decide to test should be part of the public API and should not care about implementation details.
3. Give your test a name that describes what functionality it's testing. The functionality being tested should ideally be simple enough to fit into a name, e.g. `removeNonexistentBlockThrowsException`, `mkdirCreatesDirectory`, or `cannotMkdirExistingFile`.

	    @Test
	    public void detectLostWorker() throws Exception {

4. Set up the situation you want to test. Here we register a worker and then simulate an hour passing. The `HeartbeatScheduler` section enforces that the lost worker heartbeat runs at least once.

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

5. Check that the class behaved correctly:

	      // Make sure the worker is detected as lost.
	      Set<WorkerInfo> info = mMaster.getLostWorkersInfo();
	      Assert.assertEquals(worker1, Iterables.getOnlyElement(info).getId());
	    }
6. Loop back to step #2 until the class's entire public API has been tested.


# Patterns to avoid

1. Avoid randomness. Edge cases should be handled explicitly.
2. Avoid waiting for something by calling Thread.sleep(). This leads to slower unit tests and can cause flaky failures if the sleep isn't long enough.
3. Avoid modifying global state. We typically use JUnit `Rule` when this is necessary so that the state is properly restored. See `SystemPropertyRule`, `TtlIntervalRule`, or `LocalAlluxioClusterResource` for examples of this.
4. Avoid using Whitebox to mess with the internal state of objects under test. If you need to mock a dependency, change the object to take the dependency as a parameter in its constructor (see [dependency injection](https://en.wikipedia.org/wiki/Dependency_injection))
5. Avoid slow tests. Mock expensive dependencies and aim to keep individual test times under 100ms.
