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

package alluxio.master.file;

import static org.junit.Assert.assertEquals;

import alluxio.AlluxioURI;
import alluxio.master.file.StartupConsistencyCheck.Status;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * Unit tests for {@link StartupConsistencyCheck}.
 */
public final class StartupConsistencyCheckTest {
  @Test
  public void createCompleteCheck() {
    List<AlluxioURI> uris = ImmutableList.of(new AlluxioURI("/"), new AlluxioURI("/dir"));
    StartupConsistencyCheck check = StartupConsistencyCheck.complete(uris);
    assertEquals(Status.COMPLETE, check.getStatus());
    assertEquals(uris, check.getInconsistentUris());
  }

  @Test
  public void createDisabledCheck() {
    StartupConsistencyCheck check = StartupConsistencyCheck.disabled();
    assertEquals(Status.DISABLED, check.getStatus());
    assertEquals(Collections.EMPTY_LIST, check.getInconsistentUris());
  }

  @Test
  public void createNotStartedCheck() {
    StartupConsistencyCheck check = StartupConsistencyCheck.notStarted();
    assertEquals(Status.NOT_STARTED, check.getStatus());
    assertEquals(Collections.EMPTY_LIST, check.getInconsistentUris());
  }

  @Test
  public void createFailedCheck() {
    StartupConsistencyCheck check = StartupConsistencyCheck.failed();
    assertEquals(Status.FAILED, check.getStatus());
    assertEquals(Collections.EMPTY_LIST, check.getInconsistentUris());
  }

  @Test
  public void createRunningCheck() {
    StartupConsistencyCheck check = StartupConsistencyCheck.running();
    assertEquals(Status.RUNNING, check.getStatus());
    assertEquals(Collections.EMPTY_LIST, check.getInconsistentUris());
  }
}
