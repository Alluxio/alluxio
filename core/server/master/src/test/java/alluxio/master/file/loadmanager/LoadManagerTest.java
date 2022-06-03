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

package alluxio.master.file.loadmanager;

import static alluxio.master.file.loadmanager.LoadManager.Scheduler;
import static alluxio.master.file.loadmanager.LoadManager.Load;

import alluxio.master.file.loadmanager.load.LoadInfo;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import alluxio.exception.AlluxioRuntimeException;
import alluxio.util.CommonUtils;

import com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class LoadManagerTest {
  private static final int CMD_SUCCESS_COUNT = 10;
  private static final int TIMEOUT_CMD_COUNT = 3;
  private static final int ALLUXIO_RT_COUNT = 3;
  private Scheduler mScheduler = spy(new Scheduler());;
  private final AtomicLong mLoadId = new AtomicLong();

  @Rule
  public ExpectedException mException = ExpectedException.none();

  @Test
  public void testScheduler() throws Exception {
    List<Load> successCmd = generateLoadDetailsWithException(
            CMD_SUCCESS_COUNT, Optional.empty());
    List<Load> timeoutCmd = generateLoadDetailsWithException(
            TIMEOUT_CMD_COUNT, Optional.of(TimeoutException::new));
    List<Load> alluxioRunTimeCmd = generateLoadDetailsWithException(
            ALLUXIO_RT_COUNT, Optional.of(AlluxioRuntimeException::new));

    List<Load> allCmds = Stream.of(successCmd, timeoutCmd, alluxioRunTimeCmd)
            .flatMap(List::stream).collect(Collectors.toList());;

    for (Load s: allCmds) {
      mScheduler.schedule(s);
    }

    for (Load s: allCmds) {
      verify(mScheduler).runLoad(s);
    }
  }

  private List<Load> generateLoadDetailsWithException(
          int count, Optional<Function<String, Exception>> fnOpt) throws Exception {
    List<Load> loads = Lists.newArrayList();
    for (int i = 0; i < count; i++) {
      LoadInfo info = generateRandomLoadInfo();
      Load s = generateLoads(info);
      if (fnOpt.isPresent()) {
        addException(s, fnOpt.get());
      } else {
        makeLoadRun(s);
      }
      loads.add(s);
    }
    return loads;
  }

  LoadInfo generateRandomLoadInfo() {
    return new LoadInfo(mLoadId.incrementAndGet(),
            CommonUtils.randomAlphaNumString(5), 1);
  }

  Load generateLoads(LoadInfo loadInfo) {
    return new Load(loadInfo.getId(),
            loadInfo.getPath(), loadInfo.getLoadOptions());
  }

  private void makeLoadRun(Load load) throws Exception {
    doNothing().when(mScheduler).runLoad(load);
  }

  private void addException(Load load, Function<String, Exception> fn) throws Exception {
    Throwable mThrow = generateException(fn, load.getPath()).get();
    doThrow(mThrow).when(mScheduler).runLoad(load);
  }

  private <T, E> Supplier<E> generateException(Function<T, E> fn, T v) {
    return () -> fn.apply(v);
  }
}
