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
import alluxio.resource.CloseableResource;
import alluxio.underfs.ObjectUnderFileSystem;
import alluxio.underfs.UfsClient;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsLoadResult;
import alluxio.underfs.UfsStatus;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectAttributes;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.Nullable;

public class S3Test {
  ExecutorService mThreadPool;
  TaskTracker mTaskTracker;
  MdSync mMdSync;
  UfsClient mUfsClient;
  UfsSyncPathCache mUfsSyncPathCache;
  SyncProcess mSyncProcess;
  S3AsyncClient mS3Client;
  String mBucket = "alluxiotyler";

  @Before
  public void before() {
    mThreadPool = Executors.newCachedThreadPool();
    mS3Client = S3AsyncClient.builder().region(Region.US_WEST_1).build();
    mUfsClient = new UfsClient() {
      @Override
      public void performGetStatusAsync(
          String path, Consumer<UfsLoadResult> onComplete,
          Consumer<Throwable> onError) {

        path = CommonUtils.stripPrefixIfPresent(path, AlluxioURI.SEPARATOR);
        path = path.equals(AlluxioURI.SEPARATOR) ? "" : path;
        if (path.isEmpty()) {
          onComplete.accept(new UfsLoadResult(Stream.empty(), 0, null, null, false, false));
          return;
        }
        GetObjectAttributesRequest request =
            GetObjectAttributesRequest.builder().objectAttributes(
                ObjectAttributes.E_TAG, ObjectAttributes.OBJECT_SIZE)
                .bucket(mBucket).key(path).build();
        String finalPath = path;
        mS3Client.getObjectAttributes(request).whenCompleteAsync((result, err) -> {
          if (err != null) {
            if (err.getCause() instanceof NoSuchKeyException) {
              onComplete.accept(new UfsLoadResult(Stream.empty(), 0, null, null, false, false));
            } else {
              onError.accept(err);
            }
          } else {
            Instant lastModifiedDate = result.lastModified();
            Long lastModifiedTime = lastModifiedDate == null ? null
                : lastModifiedDate.toEpochMilli();
            UfsStatus status;
            if (finalPath.endsWith(AlluxioURI.SEPARATOR)) {
              status = new UfsDirectoryStatus(finalPath, "", "", (short) 0, lastModifiedTime);
            } else {
              status = new UfsFileStatus(finalPath, result.eTag(), result.objectSize(),
                  lastModifiedTime, "", "", (short) 0, 0L);
            }
            onComplete.accept(new UfsLoadResult(Stream.of(status), 1, null,
                null, false, status.isFile()));
          }
        });
      }

      @Override
      public void performListingAsync(
          String path, @Nullable String continuationToken, @Nullable String startAfter,
          DescendantType descendantType, Consumer<UfsLoadResult> onComplete,
          Consumer<Throwable> onError) {
        path = CommonUtils.stripPrefixIfPresent(path, AlluxioURI.SEPARATOR);
        path = PathUtils.normalizePath(path, AlluxioURI.SEPARATOR);
        path = path.equals(AlluxioURI.SEPARATOR) ? "" : path;
        System.out.printf("Request path %s, token %s%n", path, continuationToken);
        mS3Client.listObjectsV2(ListObjectsV2Request.builder().prefix(path).startAfter(startAfter) // .prefix("aaa1/folder1/").delimiter("/")
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
                        result.isTruncated(), false));
              }
            });
      }
    };
    mSyncProcess = Mockito.spy(new TestSyncProcess());
    mUfsSyncPathCache = Mockito.mock(UfsSyncPathCache.class);
    mTaskTracker = new TaskTracker(
        10, 100, false, false,
        mUfsSyncPathCache, mSyncProcess, a ->
        new CloseableResource<UfsClient>(mUfsClient) {
          @Override
          public void closeResource() {
          }
        });
    mMdSync = new MdSync(mTaskTracker);
  }

  @Test
  public void stringTest() {
    System.out.println(new AlluxioURI("asdf"));

    AlluxioURI filePath = new AlluxioURI("s3:///afolder");
    System.out.println(filePath.getPath());

    AlluxioURI bucket = new AlluxioURI("s3://bucket/file/nested");
    String testString = PathUtils.normalizePath(bucket.getPath(), AlluxioURI.SEPARATOR);
    System.out.println(testString);

    System.out.println(testString.substring(0, testString.indexOf(AlluxioURI.SEPARATOR, 1)));
    System.out.println(bucket.getParent());
    System.out.println(new AlluxioURI(bucket.getRootPath()).join("somepath"));
    System.out.println(filePath.getRootPath());
    System.out.println(bucket.getScheme());
  }

  @Test
  public void s3Test() throws Throwable {
    Pair<Boolean, BaseTask> result = mTaskTracker.checkTask(mMdSync,
        new AlluxioURI("c3991175-9f1e-4ec6-8ed9-23b1370ed4ea/"), null, DescendantType.ALL, 0, DirectoryLoadType.NONE);
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
