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

package alluxio.snapshot;

import alluxio.AlluxioURI;
import alluxio.annotation.SuppressFBWarnings;
import alluxio.concurrent.jsr.CompletableFuture;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.NodeState;
import alluxio.master.AlluxioMasterProcess;
import alluxio.master.StateLockOptions;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.contexts.CompleteFileContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.journal.JournalType;
import alluxio.master.journal.raft.JournalStateMachine;
import alluxio.master.journal.raft.RaftJournalSystem;
import alluxio.master.journal.raft.RaftPrimarySelector;
import alluxio.resource.LockResource;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.junit.rules.TemporaryFolder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;

@SuppressFBWarnings("UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR")
public class SnapshotBench {
  @State(Scope.Benchmark)
  public static class Snapshot {
    TemporaryFolder mFolder = new TemporaryFolder();
    AlluxioMasterProcess mMasterProcess;
    JournalStateMachine mStateMachine;
    RaftPrimarySelector mPrimarySelector;
    CompletableFuture<Void> mLifeCycle;

    @Setup(Level.Trial)
    public void setup() throws Exception {
      LogManager.getRootLogger().setLevel(org.apache.log4j.Level.ERROR);

      mFolder.create();
      Configuration.set(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED);
      Configuration.set(PropertyKey.MASTER_JOURNAL_FOLDER, mFolder.newFolder("journal"));
      Configuration.set(PropertyKey.MASTER_METASTORE_DIR, mFolder.newFolder("metastore"));
      Configuration.set(PropertyKey.SECURITY_AUTHENTICATION_TYPE, "NOSASL");
      Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, false);
      mMasterProcess = AlluxioMasterProcess.Factory.create();
      RaftJournalSystem journalSystem = (RaftJournalSystem) mMasterProcess
          .getMaster(FileSystemMaster.class).getMasterContext().getJournalSystem();
      mPrimarySelector = (RaftPrimarySelector) journalSystem.getPrimarySelector();
      mLifeCycle = CompletableFuture.runAsync(() -> {
        try {
          mMasterProcess.start();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
      mMasterProcess.waitForReady(10_000);
      Field f = journalSystem.getClass().getDeclaredField("mStateMachine");
      f.setAccessible(true);
      mStateMachine = (JournalStateMachine) f.get(journalSystem);

      FileSystemMaster master = mMasterProcess.getMaster(FileSystemMaster.class);
      for (int i = 0; i < 1_000_000; i++) {
        if (i % 100_000 == 0) {
          System.out.printf("Creating file%d%n", i);
        }
        AlluxioURI alluxioURI = new AlluxioURI("/file" + i);
        master.createFile(alluxioURI, CreateFileContext.defaults());
        master.completeFile(alluxioURI, CompleteFileContext.defaults());
      }
      System.out.println("getting state lock");
      LockResource lr = master.getMasterContext().getStateLockManager()
          .lockExclusive(StateLockOptions.defaults());
      lr.close();
      System.out.println("Setup complete");
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
      mMasterProcess.stop();
      mPrimarySelector.notifyStateChanged(NodeState.STANDBY);
      mLifeCycle.join();
      mFolder.delete();
      System.out.println("Tear down complete");
    }

    @TearDown(Level.Invocation)
    public void tearDownIteration() throws IOException {
      File snapshotDir = mStateMachine.getStateMachineStorage().getSnapshotDir();
      FileUtils.cleanDirectory(snapshotDir);
    }
  }

  @Benchmark
  @Warmup(iterations = 0)
  @Measurement(iterations = 1)
  public void snapshot(Blackhole bh, Snapshot snapshot) {
    System.out.println("Taking snapshot");
    bh.consume(snapshot.mStateMachine.takeLocalSnapshot(true));
    System.out.println("Took snapshot");
  }

  public static void main(String[] args) throws Exception {
    Options argsCli = new CommandLineOptions(args);
    Options opts = new OptionsBuilder()
        .forks(0)
        .parent(argsCli)
        .include(SnapshotBench.class.getName())
        .result("results.json")
        .resultFormat(ResultFormatType.JSON)
        .build();
    new Runner(opts).run();
  }
}
