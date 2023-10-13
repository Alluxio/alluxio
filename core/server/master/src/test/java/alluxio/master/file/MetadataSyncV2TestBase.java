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
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.collections.Pair;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.file.options.DirectoryLoadType;
import alluxio.grpc.ExistsPOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.master.file.contexts.ExistsContext;
import alluxio.master.file.contexts.GetStatusContext;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.file.mdsync.SyncFailReason;
import alluxio.master.file.mdsync.SyncOperation;
import alluxio.master.file.mdsync.TaskGroup;
import alluxio.master.file.mdsync.TaskInfo;
import alluxio.master.file.mdsync.TaskStats;
import alluxio.util.io.PathUtils;
import alluxio.wire.FileInfo;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.gaul.s3proxy.S3Proxy;
import org.gaul.s3proxy.junit.S3ProxyJunitCore;
import org.gaul.s3proxy.junit.S3ProxyRule;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MetadataSyncV2TestBase extends FileSystemMasterTestBase {
  static final Logger LOG = LoggerFactory.getLogger(FileSystemMetadataSyncV2Test.class);
  static final String TEST_BUCKET = "alluxio-mdsync-test-bucket";
  static final String TEST_BUCKET2 = "alluxio-mdsync-test-bucket-2";
  static final String TEST_FILE = "test_file";
  static final String TEST_DIRECTORY = "test_directory";
  static final String TEST_CONTENT = "test_content";
  static final String TEST_CONTENT_MODIFIED = "test_content_modified";
  static final AlluxioURI UFS_ROOT = new AlluxioURI("s3://" + TEST_BUCKET + "/");
  static final AlluxioURI UFS_ROOT2 = new AlluxioURI("s3://" + TEST_BUCKET2 + "/");
  static final AlluxioURI MOUNT_POINT = new AlluxioURI("/s3_mount");
  static final AlluxioURI MOUNT_POINT2 = new AlluxioURI("/s3_mount2");
  static final AlluxioURI NESTED_MOUNT_POINT = new AlluxioURI("/mnt/nested_s3_mount");
  static final AlluxioURI NESTED_S3_MOUNT_POINT =
      new AlluxioURI("/s3_mount/nested_s3_mount");
  static final long TIMEOUT_MS = 30_000;

  @Rule
  public S3ProxyRule mS3Proxy = S3ProxyRule.builder()
      .withBlobStoreProvider("transient")
      .withCredentials("_", "_")
      .build();

  boolean mUseRealS3 = false;
  AmazonS3 mS3Client;
  S3Client mClient;
  DirectoryLoadType mDirectoryLoadType;

  @Override
  public void before() throws Exception {
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, false);
    Configuration.set(PropertyKey.UNDERFS_LISTING_LENGTH, 2);

    if (mUseRealS3) {
      Configuration.set(PropertyKey.UNDERFS_S3_REGION, "us-west-1");
      mClient = S3Client.builder().region(Region.US_WEST_1).build();
      mS3Client = AmazonS3ClientBuilder.standard()
          .withRegion(Region.US_WEST_1.toString()).build();
    } else {
      Configuration.set(PropertyKey.UNDERFS_S3_ENDPOINT,
          mS3Proxy.getUri().getHost() + ":" + mS3Proxy.getUri().getPort());
      Configuration.set(PropertyKey.UNDERFS_S3_ENDPOINT_REGION, "us-west-2");
      Configuration.set(PropertyKey.UNDERFS_S3_DISABLE_DNS_BUCKETS, true);
      Configuration.set(PropertyKey.S3A_ACCESS_KEY, mS3Proxy.getAccessKey());
      Configuration.set(PropertyKey.S3A_SECRET_KEY, mS3Proxy.getSecretKey());
      mClient = S3Client.builder().credentialsProvider(StaticCredentialsProvider.create(
              AwsBasicCredentials.create(mS3Proxy.getAccessKey(), mS3Proxy.getSecretKey())))
          .endpointOverride(mS3Proxy.getUri()).region(Region.US_WEST_2)
          .build();

      mS3Client = AmazonS3ClientBuilder
          .standard()
          .withPathStyleAccessEnabled(true)
          .withCredentials(
              new AWSStaticCredentialsProvider(
                  new BasicAWSCredentials(mS3Proxy.getAccessKey(), mS3Proxy.getSecretKey())))
          .withEndpointConfiguration(
              new AwsClientBuilder.EndpointConfiguration(mS3Proxy.getUri().toString(),
                  Regions.US_WEST_2.getName()))
          .build();
    }
    mS3Client.createBucket(TEST_BUCKET);
    mS3Client.createBucket(TEST_BUCKET2);
    super.before();
  }

  @Override
  public void after() throws Exception {
    mS3Client.shutdown();
    mClient.close();
    try {
      stopS3Server();
    } catch (Exception e) {
      LOG.error("Closing s3 mock server failed", e);
    }
    super.after();
  }

  ListStatusContext listSync(boolean isRecursive) {
    return ListStatusContext.mergeFrom(ListStatusPOptions.newBuilder()
        .setRecursive(isRecursive)
        .setLoadMetadataType(LoadMetadataPType.ALWAYS)
        .setCommonOptions(
            FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0).build()
        ));
  }

  ListStatusContext listNoSync(boolean isRecursive) {
    return ListStatusContext.mergeFrom(ListStatusPOptions.newBuilder()
        .setRecursive(isRecursive)
        .setLoadMetadataType(LoadMetadataPType.NEVER)
        .setCommonOptions(
            FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1).build()
        ));
  }

  GetStatusContext getNoSync() {
    return GetStatusContext.mergeFrom(GetStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER)
        .setCommonOptions(
            FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1).build()
        ));
  }

  static ExistsContext existsNoSync() {
    return ExistsContext.mergeFrom(ExistsPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER)
        .setCommonOptions(
            FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1).build()
        ));
  }

  void stopS3Server() {
    try {
      Field coreField = S3ProxyRule.class.getDeclaredField("core");
      coreField.setAccessible(true);
      S3ProxyJunitCore core = (S3ProxyJunitCore) coreField.get(mS3Proxy);
      Field s3ProxyField = S3ProxyJunitCore.class.getDeclaredField("s3Proxy");
      s3ProxyField.setAccessible(true);
      S3Proxy proxy = (S3Proxy) s3ProxyField.get(core);
      proxy.stop();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  void startS3Server() {
    try {
      Field coreField = S3ProxyRule.class.getDeclaredField("core");
      coreField.setAccessible(true);
      S3ProxyJunitCore core = (S3ProxyJunitCore) coreField.get(mS3Proxy);
      Field s3ProxyField = S3ProxyJunitCore.class.getDeclaredField("s3Proxy");
      s3ProxyField.setAccessible(true);
      S3Proxy proxy = (S3Proxy) s3ProxyField.get(core);
      proxy.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void checkUfsMatches(
      AlluxioURI alluxioPath, String s3Bucket,
      String mountPrefix,
      DefaultFileSystemMaster master, S3Client s3client) throws Exception {

    Stack<Pair<String, String>> toCheck = new Stack<>();
    toCheck.push(new Pair<>(alluxioPath.getPath(), mountPrefix));
    while (!toCheck.isEmpty()) {
      Pair<String, String> nxt = toCheck.pop();

      Iterator<FileInfo> alluxioItems = master.listStatus(new AlluxioURI(nxt.getFirst()),
          ListStatusContext.defaults().disableMetadataSync()).stream().iterator();
      Iterator<Pair<String, String>> ufsItems = listUfsPath(s3Bucket, nxt.getSecond(), s3client,
          mountPrefix, alluxioPath.getPath());
      while (alluxioItems.hasNext()) {
        FileInfo nxtAlluxio = alluxioItems.next();
        if (!ufsItems.hasNext()) {
          throw new IllegalStateException(
              String.format("Ufs did not find alluxio item %s", nxtAlluxio));
        }
        Pair<String, String> nxtUfs = ufsItems.next();
        String nxtInode = nxtAlluxio.getPath();
        if (nxtAlluxio.isFolder()) {
          toCheck.push(new Pair<>(nxtAlluxio.getPath(), nxtUfs.getSecond()));
          nxtInode = PathUtils.normalizePath(nxtInode, AlluxioURI.SEPARATOR);
        }
        // System.out.printf("Checking %s, %s%n", nxtInode, nxtUfs.getFirst());
        assertEquals(nxtInode, nxtUfs.getFirst());
      }
      if (ufsItems.hasNext()) {
        throw new IllegalStateException(
            String.format("alluxio did not find ufs item %s", ufsItems.next()));
      }
    }
  }

  static Iterator<Pair<String, String>> listUfsPath(
      String s3Bucket, String s3Path, S3Client client,
      String mountPrefix, String alluxioPrefix) {
    String normalizedPrefix = PathUtils.normalizePath(alluxioPrefix, AlluxioURI.SEPARATOR);
    if (!s3Path.isEmpty()) {
      s3Path = PathUtils.normalizePath(s3Path, AlluxioURI.SEPARATOR);
    }
    if (!mountPrefix.isEmpty()) {
      mountPrefix = PathUtils.normalizePath(mountPrefix, AlluxioURI.SEPARATOR);
    }
    ListObjectsV2Iterable result = client.listObjectsV2Paginator(ListObjectsV2Request.builder()
        .bucket(s3Bucket).delimiter(AlluxioURI.SEPARATOR).prefix(s3Path).build());
    String finalMountPrefix = mountPrefix;
    String finalS3Path = s3Path;
    return result.stream().flatMap(resp ->
            Stream.concat(resp.commonPrefixes().stream().map(CommonPrefix::prefix),
                resp.contents().stream().map(S3Object::key)))
        .filter(nxt -> {
          assertTrue(nxt.startsWith(finalS3Path));
          return nxt.length() > finalS3Path.length();
        }).sorted().distinct()
        .map(nxt -> new Pair<>(
            normalizedPrefix + nxt.substring(finalMountPrefix.length()), nxt)).iterator();
  }

  static void assertSyncOperations(TaskInfo taskInfo, Map<SyncOperation, Long> operations) {
    assertSyncOperations(taskInfo.getStats().getSuccessOperationCount(), operations);
  }

  static void assertSyncOperations(TaskGroup taskGroup, Map<SyncOperation, Long> operations) {
    AtomicLong[] stats = new AtomicLong[SyncOperation.values().length];
    for (int i = 0; i < stats.length; ++i) {
      stats[i] = new AtomicLong();
    }
    taskGroup.getTasks().forEach(
        it -> {
          AtomicLong[] taskStats = it.getTaskInfo().getStats().getSuccessOperationCount();
          for (int i = 0; i < taskStats.length; ++i) {
            stats[i].addAndGet(taskStats[i].get());
          }
        }
    );
    assertSyncOperations(stats, operations);
  }

  private static void assertSyncOperations(
      AtomicLong[] stats, Map<SyncOperation, Long> operations) {
    for (SyncOperation operation : SyncOperation.values()) {
      assertEquals(
          "Operation " + operation.toString() + " count not equal. "
              + "Actual operation count: "
              + Arrays.toString(stats),
          (long) operations.getOrDefault(operation, 0L),
          stats[operation.getValue()].get()
      );
    }
  }

  static void assertSyncFailureReason(TaskInfo taskInfo, SyncFailReason failReason) {
    Map<Long, TaskStats.SyncFailure> failReasons = taskInfo.getStats().getSyncFailReasons();
    assertEquals(1, failReasons.size());
    assertTrue(failReasons.entrySet().stream().map(it -> it.getValue().getSyncFailReason()).collect(
        Collectors.toList()).contains(failReason));
  }
}
