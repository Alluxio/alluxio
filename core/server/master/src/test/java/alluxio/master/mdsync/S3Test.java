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

import static org.junit.Assert.assertFalse;

import alluxio.AlluxioURI;
import alluxio.collections.Pair;
import alluxio.file.options.DescendantType;
import alluxio.master.file.meta.UfsSyncPathCache;
import alluxio.underfs.UfsFileStatus;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class S3Test {
  ExecutorService mThreadPool;
  TaskTracker mTaskTracker;
  MdSync mMdSync;
  UfsClient mUfsClient;
  UfsSyncPathCache mUfsSyncPathCache;
  SyncProcess mSyncProcess;
  S3AsyncClient mS3Client;

  @Before
  public void before() {
    mThreadPool = Executors.newCachedThreadPool();
    mS3Client = S3AsyncClient.builder().region(Region.US_WEST_1).build();
    mUfsClient = (path, continuationToken, descendantType, onComplete, onError)
        -> {
      System.out.printf("Request path %s, token %s%n", path, continuationToken);
      mS3Client.listObjectsV2(ListObjectsV2Request.builder()
              .continuationToken(continuationToken).bucket("alluxiotyler2").build())
          .whenCompleteAsync((result, err) -> {
            if (err != null) {
              System.out.println("got error");
              onError.accept(err);
            } else {
              System.out.printf("got result, cont token %s%n", result.nextContinuationToken());
              onComplete.accept(
                  new UfsLoadResult(result.contents().stream().map(item -> new UfsFileStatus(
                      item.key(), item.eTag(), item.size(),
                      item.lastModified() == null ? null : item.lastModified().toEpochMilli(),
                      "", "", (short) 0, 0L)),
                      result.keyCount(), result.nextContinuationToken(), result.isTruncated()));
            }
          });
    };
    mSyncProcess = Mockito.spy(new SyncProcess());
    mUfsSyncPathCache = Mockito.mock(UfsSyncPathCache.class);
    mTaskTracker = new TaskTracker(
        10, 100, false, false,
        mUfsSyncPathCache, mSyncProcess);
    mMdSync = new MdSync(mTaskTracker, a -> a, a -> mUfsClient);
  }

  @Test
  public void s3Test() throws Exception {
    Pair<Boolean, BaseTask> result = mTaskTracker.checkTask(mMdSync,
        new AlluxioURI("/"), DescendantType.ALL, 0, DirectoryLoadType.NONE);
    result.getSecond().waitComplete(0);
    System.out.println(result.getSecond().getTaskInfo().getStats());
  }

  @After
  public void after() throws Exception {
    mS3Client.close();
    assertFalse(mTaskTracker.hasRunningTasks());
    mTaskTracker.close();
    mThreadPool.shutdown();
  }
}
