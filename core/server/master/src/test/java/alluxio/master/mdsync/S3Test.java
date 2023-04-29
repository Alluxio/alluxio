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
import alluxio.file.options.DirectoryLoadType;
import alluxio.master.file.meta.UfsAbsentPathCache;
import alluxio.master.file.meta.UfsSyncPathCache;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UfsClient;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsLoadResult;
import alluxio.util.CommonUtils;
import alluxio.util.RateLimiter;
import alluxio.util.io.PathUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import javax.annotation.Nullable;

@Ignore
public class S3Test {
  ExecutorService mThreadPool;
  TaskTracker mTaskTracker;
  MdSync mMdSync;
  UfsClient mUfsClient;
  UfsSyncPathCache mUfsSyncPathCache;
  UfsAbsentPathCache mAbsentCache;
  SyncProcess mSyncProcess;
  S3AsyncClient mS3Client;
  String mBucket = "alluxiotyler";
  RateLimiter mRateLimiter = RateLimiter.createRateLimiter(10);

  @Before
  public void before() {
    mThreadPool = Executors.newCachedThreadPool();
    mS3Client = S3AsyncClient.builder().region(Region.US_WEST_1).build();
    mUfsClient = new UfsClient() {

      @Override
      public void performListingAsync(
          String path, @Nullable String continuationToken, @Nullable String startAfter,
          DescendantType descendantType, boolean checkStatus,
          Consumer<UfsLoadResult> onComplete, Consumer<Throwable> onError) {
        path = CommonUtils.stripPrefixIfPresent(path, AlluxioURI.SEPARATOR);
        path = PathUtils.normalizePath(path, AlluxioURI.SEPARATOR);
        path = path.equals(AlluxioURI.SEPARATOR) ? "" : path;
        System.out.printf("Request path %s, token %s%n", path, continuationToken);
        // .prefix("aaa1/folder1/").delimiter("/")
        mS3Client.listObjectsV2(ListObjectsV2Request.builder().prefix(path).startAfter(startAfter)
                .continuationToken(continuationToken).bucket(mBucket).build())
            .whenCompleteAsync((result, err) -> {
              if (err != null) {
                System.out.println("got error");
                onError.accept(err);
              } else {
                // System.out.printf("got result, cont token %s%n", result.nextContinuationToken());
                if (result.commonPrefixes().size() != 0) {
                  System.out.printf("got common prefixes %s",
                      Arrays.toString(new List[]{result.commonPrefixes()}));
                }
                AlluxioURI lastItem = null;
                String lastPrefix = result.commonPrefixes().size() == 0 ? null
                    : result.commonPrefixes().get(result.commonPrefixes().size() - 1).prefix();
                String lastResult = result.contents().size() == 0 ? null
                    : result.contents().get(result.contents().size() - 1).key();
                if (lastPrefix == null && lastResult != null) {
                  lastItem = new AlluxioURI(lastResult);
                } else if (lastPrefix != null && lastResult == null) {
                  lastItem = new AlluxioURI(lastPrefix);
                } else if (lastPrefix != null) { // both are non-null
                  lastItem = new AlluxioURI(lastPrefix.compareTo(lastResult) > 0
                      ? lastPrefix : lastResult);
                }
                System.out.printf("got last item %s%n", lastItem.getPath());
                onComplete.accept(
                    new UfsLoadResult(result.contents().stream().map(item -> new UfsFileStatus(
                        item.key(), item.eTag(), item.size(),
                        item.lastModified() == null ? null : item.lastModified().toEpochMilli(),
                        "", "", (short) 0, 0L)),
                        result.keyCount(), result.nextContinuationToken(), lastItem,
                        result.isTruncated(), false, true));
              }
            });
      }

      @Override
      public RateLimiter getRateLimiter() {
        return mRateLimiter;
      }
    };
    mSyncProcess = Mockito.spy(new TestSyncProcess());
    mUfsSyncPathCache = Mockito.mock(UfsSyncPathCache.class);
    mAbsentCache = Mockito.mock(UfsAbsentPathCache.class);
    mTaskTracker = new TaskTracker(
        10, 100, false, false,
        mUfsSyncPathCache, mAbsentCache, mSyncProcess, a ->
        new CloseableResource<UfsClient>(mUfsClient) {
          @Override
          public void closeResource() {
          }
        });
    mMdSync = new MdSync(mTaskTracker);
  }

  @Test
  public void s3Test() throws Throwable {
    Pair<Boolean, BaseTask> result = mTaskTracker.checkTask(mMdSync,
        new AlluxioURI("/"), new AlluxioURI("/"),
        null, DescendantType.ALL, 0, DirectoryLoadType.SINGLE_LISTING);
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
