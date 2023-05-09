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

package alluxio.master.file.mdsync;

import static alluxio.file.options.DescendantType.ALL;
import static alluxio.file.options.DescendantType.NONE;
import static alluxio.file.options.DescendantType.ONE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.file.options.DirectoryLoadType;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UfsClient;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Clock;
import java.util.function.Function;

public class BaseTaskTest {

  private MetadataSyncHandler mMetadataSyncHandler;

  private final Clock mClock = Clock.systemUTC();

  private final MockUfsClient mUfsClient = new MockUfsClient();

  private final Function<AlluxioURI, CloseableResource<UfsClient>> mClientSupplier =
      (uri) -> new CloseableResource<UfsClient>(mUfsClient) {
        @Override
        public void closeResource() {}
      };

  @Before
  public void before() {
    mMetadataSyncHandler = new MetadataSyncHandler(Mockito.mock(TaskTracker.class), null, null);
  }

  @Test
  public void PathIsCoveredNone() {
    BaseTask path = BaseTask.create(new TaskInfo(mMetadataSyncHandler, new AlluxioURI("/path"),
        new AlluxioURI("/path"), null,
        NONE, 0, DirectoryLoadType.SINGLE_LISTING, 0), mClock.millis(), mClientSupplier);
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
    BaseTask path = BaseTask.create(new TaskInfo(mMetadataSyncHandler, new AlluxioURI("/path"),
        new AlluxioURI("/path"), null,
        ONE, 0, DirectoryLoadType.SINGLE_LISTING, 0), mClock.millis(), mClientSupplier);
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
    BaseTask path = BaseTask.create(new TaskInfo(mMetadataSyncHandler, new AlluxioURI("/path"),
        new AlluxioURI("/path"), null,
        ALL, 0, DirectoryLoadType.SINGLE_LISTING, 0), mClock.millis(), mClientSupplier);
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
