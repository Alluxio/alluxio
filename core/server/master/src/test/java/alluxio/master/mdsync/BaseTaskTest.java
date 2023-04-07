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

package alluxio.master.mdsync;

import static alluxio.file.options.DescendantType.ALL;
import static alluxio.file.options.DescendantType.NONE;
import static alluxio.file.options.DescendantType.ONE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Clock;

public class BaseTaskTest {

  private MdSync mMdSync;

  private final Clock mClock = Clock.systemUTC();

  @Before
  public void before() {
    mMdSync = new MdSync(Mockito.mock(TaskTracker.class));
  }

  @Test
  public void PathIsCoveredNone() {
    BaseTask path = BaseTask.create(new TaskInfo(mMdSync, new AlluxioURI("/path"),
        new AlluxioURI("/path"), null,
        NONE, 0, DirectoryLoadType.NONE, 0), mClock.millis(), a -> null);
    assertTrue(path.pathIsCovered(new AlluxioURI("/path"), NONE));
    assertFalse(path.pathIsCovered(new AlluxioURI("/"), NONE));
    assertFalse(path.pathIsCovered(new AlluxioURI("/p"), NONE));
    assertFalse(path.pathIsCovered(new AlluxioURI("/path2"), NONE));
    assertFalse(path.pathIsCovered(new AlluxioURI("/path/nested"), NONE));

    assertFalse(path.pathIsCovered(new AlluxioURI("/path"), ONE));
    assertFalse(path.pathIsCovered(new AlluxioURI("/"), ONE));
    assertFalse(path.pathIsCovered(new AlluxioURI("/p"), ONE));
    assertFalse(path.pathIsCovered(new AlluxioURI("/path2"), ONE));
    assertFalse(path.pathIsCovered(new AlluxioURI("/path/nested"), ONE));

    assertFalse(path.pathIsCovered(new AlluxioURI("/path"), ALL));
    assertFalse(path.pathIsCovered(new AlluxioURI("/"), ALL));
    assertFalse(path.pathIsCovered(new AlluxioURI("/p"), ALL));
    assertFalse(path.pathIsCovered(new AlluxioURI("/path2"), ALL));
    assertFalse(path.pathIsCovered(new AlluxioURI("/path/nested"), ALL));
  }

  @Test
  public void PathIsCoveredOne() {
    BaseTask path = BaseTask.create(new TaskInfo(mMdSync, new AlluxioURI("/path"),
        new AlluxioURI("/path"), null,
        ONE, 0, DirectoryLoadType.NONE, 0), mClock.millis(), a -> null);
    assertTrue(path.pathIsCovered(new AlluxioURI("/path"), NONE));
    assertFalse(path.pathIsCovered(new AlluxioURI("/"), NONE));
    assertFalse(path.pathIsCovered(new AlluxioURI("/p"), NONE));
    assertFalse(path.pathIsCovered(new AlluxioURI("/path2"), NONE));
    assertTrue(path.pathIsCovered(new AlluxioURI("/path/nested"), NONE));
    assertFalse(path.pathIsCovered(new AlluxioURI("/path/nested/nested"), NONE));

    assertTrue(path.pathIsCovered(new AlluxioURI("/path"), ONE));
    assertFalse(path.pathIsCovered(new AlluxioURI("/"), ONE));
    assertFalse(path.pathIsCovered(new AlluxioURI("/p"), ONE));
    assertFalse(path.pathIsCovered(new AlluxioURI("/path2"), ONE));
    assertFalse(path.pathIsCovered(new AlluxioURI("/path/nested"), ONE));
    assertFalse(path.pathIsCovered(new AlluxioURI("/path/nested/nested"), ONE));

    assertFalse(path.pathIsCovered(new AlluxioURI("/path"), ALL));
    assertFalse(path.pathIsCovered(new AlluxioURI("/"), ALL));
    assertFalse(path.pathIsCovered(new AlluxioURI("/p"), ALL));
    assertFalse(path.pathIsCovered(new AlluxioURI("/path2"), ALL));
    assertFalse(path.pathIsCovered(new AlluxioURI("/path/nested"), ALL));
    assertFalse(path.pathIsCovered(new AlluxioURI("/path/nested/nested"), ALL));
  }

  @Test
  public void PathIsCoveredAll() {
    BaseTask path = BaseTask.create(new TaskInfo(mMdSync, new AlluxioURI("/path"),
        new AlluxioURI("/path"), null,
        ALL, 0, DirectoryLoadType.NONE, 0), mClock.millis(), a -> null);
    assertTrue(path.pathIsCovered(new AlluxioURI("/path"), NONE));
    assertFalse(path.pathIsCovered(new AlluxioURI("/"), NONE));
    assertFalse(path.pathIsCovered(new AlluxioURI("/p"), NONE));
    assertFalse(path.pathIsCovered(new AlluxioURI("/path2"), NONE));
    assertTrue(path.pathIsCovered(new AlluxioURI("/path/nested"), NONE));
    assertTrue(path.pathIsCovered(new AlluxioURI("/path/nested/nested"), NONE));

    assertTrue(path.pathIsCovered(new AlluxioURI("/path"), ONE));
    assertFalse(path.pathIsCovered(new AlluxioURI("/"), ONE));
    assertFalse(path.pathIsCovered(new AlluxioURI("/p"), ONE));
    assertFalse(path.pathIsCovered(new AlluxioURI("/path2"), ONE));
    assertTrue(path.pathIsCovered(new AlluxioURI("/path/nested"), ONE));
    assertTrue(path.pathIsCovered(new AlluxioURI("/path/nested/nested"), ONE));

    assertTrue(path.pathIsCovered(new AlluxioURI("/path"), ALL));
    assertFalse(path.pathIsCovered(new AlluxioURI("/"), ALL));
    assertFalse(path.pathIsCovered(new AlluxioURI("/p"), ALL));
    assertFalse(path.pathIsCovered(new AlluxioURI("/path2"), ALL));
    assertTrue(path.pathIsCovered(new AlluxioURI("/path/nested"), ALL));
    assertTrue(path.pathIsCovered(new AlluxioURI("/path/nested/nested"), ALL));
  }
}
