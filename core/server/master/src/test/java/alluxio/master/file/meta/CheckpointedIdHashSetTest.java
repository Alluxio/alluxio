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

package alluxio.master.file.meta;

import alluxio.master.journal.checkpoint.CheckpointInputStream;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class CheckpointedIdHashSetTest {
  @Parameterized.Parameters
  public static Collection<CheckpointedIdHashSet> data() {
    return Arrays.asList(new PinnedInodeFileIds(), new ReplicationLimitedFileIds(),
        new ToBePersistedFileIds());
  }

  @Parameterized.Parameter
  public CheckpointedIdHashSet mIdHashSet;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Test
  public void test() throws IOException {
    for (long i = 0L; i < 1_000_000L; i += 5762L) {
      mIdHashSet.add(i);
    }
    List<Long> copyList = new ArrayList<>(mIdHashSet);
    File file = mFolder.newFile();
    try (OutputStream outputStream = Files.newOutputStream(file.toPath())) {
      mIdHashSet.writeToCheckpoint(outputStream);
    }
    mIdHashSet.clear();
    try (CheckpointInputStream inputStream =
             new CheckpointInputStream(Files.newInputStream(file.toPath()))) {
      mIdHashSet.restoreFromCheckpoint(inputStream);
    }
    Assert.assertTrue(mIdHashSet.containsAll(copyList));
  }
}
