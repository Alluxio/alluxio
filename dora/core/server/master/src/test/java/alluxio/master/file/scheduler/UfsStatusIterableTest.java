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

package alluxio.master.file.scheduler;

import static org.junit.Assert.assertEquals;

import alluxio.conf.Configuration;
import alluxio.master.job.UfsStatusIterable;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;

import com.google.common.base.Predicates;
import org.apache.commons.compress.utils.Lists;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Optional;

public class UfsStatusIterableTest {
  private String mLocalUfsRoot;
  private UnderFileSystem mLocalUfs;

  @Rule
  public TemporaryFolder mTemporaryFolder = new TemporaryFolder();

  @Before
  public void before() throws IOException {
    mLocalUfsRoot = mTemporaryFolder.getRoot().getAbsolutePath();
    mLocalUfs = UnderFileSystem.Factory.create(mLocalUfsRoot,
        UnderFileSystemConfiguration.defaults(Configuration.global()));
  }

  @Test
  public void testLocalFiles() throws IOException {
    // test UfsStatusIterable with local files
    mTemporaryFolder.newFile("a");
    mTemporaryFolder.newFile("b");
    mTemporaryFolder.newFolder("test");
    mTemporaryFolder.newFile("/test/b");

    UfsStatusIterable ufsStatusIterable =
        new UfsStatusIterable(mLocalUfs, mLocalUfsRoot, Optional.empty(), Predicates.alwaysTrue());
    Iterator<UfsStatus> iterator = ufsStatusIterable.iterator();
    ArrayList<UfsStatus> array = Lists.newArrayList(iterator);
    assertEquals(4, array.size());
  }
}
