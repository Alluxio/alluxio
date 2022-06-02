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

package alluxio.master.file.cmdmanager;

import static alluxio.master.file.cmdmanager.FileSystemCommandManager.Scheduler;
import static alluxio.master.file.cmdmanager.FileSystemCommandManager.CommandStatus;
import static alluxio.master.file.cmdmanager.FileSystemCommandManager.Client;

import alluxio.master.file.cmdmanager.command.CmdType;
import alluxio.master.file.cmdmanager.command.CommandInfo;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import alluxio.exception.AlluxioRuntimeException;
import alluxio.util.CommonUtils;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class FileSystemCommandManagerTest {
  private static final int CMD_SUCCESS_COUNT = 10;
  private static final int TIMEOUT_CMD_COUNT = 3;
  private static final int ALLUXIO_RT_COUNT = 3;
  private final Client mClient = mock(Client.class);
  private Scheduler mScheduler;
  private final AtomicLong mId = new AtomicLong();
  private final AtomicLong mCommandId = new AtomicLong();

  @Rule
  public ExpectedException mException = ExpectedException.none();

  @Before
  public void before() throws Exception {
    mScheduler = new Scheduler(mClient);
    mScheduler.start();
  }

  @Test
  public void testScheduler() throws Exception {
    List<CommandStatus> successCmd = generateCommandDetailsWithException(
            CMD_SUCCESS_COUNT, Optional.empty());
    List<CommandStatus> timeoutCmd = generateCommandDetailsWithException(
            TIMEOUT_CMD_COUNT, Optional.of(TimeoutException::new));
    List<CommandStatus> alluxioRunTimeCmd = generateCommandDetailsWithException(
            ALLUXIO_RT_COUNT, Optional.of(AlluxioRuntimeException::new));

    List<CommandStatus> allCmds = Stream.of(successCmd, timeoutCmd, alluxioRunTimeCmd)
            .flatMap(List::stream).collect(Collectors.toList());;

    for (CommandStatus s: allCmds) {
      mScheduler.schedule(s);
    }

    Thread.sleep(10000);

    for (CommandStatus s: allCmds) {
      verify(mClient).runCommand(s);
    }

    Assert.assertEquals(mScheduler.getRTCounts(), ALLUXIO_RT_COUNT);
    Assert.assertEquals(mScheduler.getTimeoutCounts(), TIMEOUT_CMD_COUNT);
  }

  private List<CommandStatus> generateCommandDetailsWithException(
          int count, Optional<Function<String, Exception>> fnOpt) throws Exception {
    List<CommandStatus> commandStatuses = Lists.newArrayList();
    for (int i = 0; i < count; i++) {
      CommandInfo info = generateRandomCommandInfo();
      CommandStatus s = generateCommandStatus(info);
      if (fnOpt.isPresent()) {
        addException(s, fnOpt.get());
      } else {
        makeCommandRun(s);
      }
      commandStatuses.add(s);
    }
    return commandStatuses;
  }

  CommandInfo generateRandomCommandInfo() {
    return new CommandInfo(mCommandId.incrementAndGet(),
            CommonUtils.randomAlphaNumString(5),
            CmdType.LoadCmd, 1);
  }

  CommandStatus generateCommandStatus(CommandInfo commandInfo) {
    return new CommandStatus(commandInfo.getId(),
            commandInfo.getPath(), commandInfo.getCmdOptions());
  }

  private void makeCommandRun(CommandStatus commandStatus) throws Exception {
    doNothing().when(mClient).runCommand(commandStatus);
  }

  private void addException(CommandStatus commandStatus, Function<String, Exception> fn) throws Exception {
    Throwable mThrow = generateException(fn, commandStatus.getPath()).get();
    doThrow(mThrow).when(mClient).runCommand(commandStatus);
  }

  private <T, E> Supplier<E> generateException(Function<T, E> fn, T v) {
    return () -> fn.apply(v);
  }
}
