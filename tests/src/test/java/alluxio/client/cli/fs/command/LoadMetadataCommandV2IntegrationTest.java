package alluxio.client.cli.fs.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.SystemErrRule;
import alluxio.SystemOutRule;
import alluxio.cli.fs.FileSystemShell;
import alluxio.cli.job.JobShell;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.concurrent.jsr.CompletableFuture;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.WritePType;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.master.job.JobMaster;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.io.PathUtils;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import org.gaul.s3proxy.junit.S3ProxyRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class LoadMetadataCommandV2IntegrationTest extends BaseIntegrationTest {
  private static String TEST_BUCKET = "test-bucket";
  private static final String TEST_CONTENT = "TestContents";
  private static final String TEST_FILE = "test_file";
  private static final int USER_QUOTA_UNIT_BYTES = 1000;
  public ByteArrayOutputStream mOutput = new ByteArrayOutputStream();
  public ByteArrayOutputStream mErrOutput = new ByteArrayOutputStream();
  public ExpectedException mException = ExpectedException.none();

  @Rule
  public SystemOutRule r = new SystemOutRule(mOutput);
  // public SystemOutRule r = new SystemOutRule(System.out);

  @Rule
  public SystemErrRule mErrRule = new SystemErrRule(mErrOutput);

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Rule
  public S3ProxyRule mS3Proxy = S3ProxyRule.builder()
      .withPort(8001)
      .withCredentials("_", "_")
      .build();


  // Var for Shell Test
  public LocalAlluxioCluster mLocalAlluxioCluster;
  public FileSystem mFileSystem;
  public FileSystemShell mFsShell;
  protected JobMaster mJobMaster;
  protected LocalAlluxioJobCluster mLocalAlluxioJobCluster;
  protected JobShell mJobShell;

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource = new LocalAlluxioClusterResource.Builder()
            .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, USER_QUOTA_UNIT_BYTES)
            .setProperty(PropertyKey.UNDERFS_S3_ENDPOINT, "localhost:8001")
            .setProperty(PropertyKey.UNDERFS_S3_ENDPOINT_REGION, "us-west-2")
            .setProperty(PropertyKey.UNDERFS_S3_DISABLE_DNS_BUCKETS, true)
            .setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, "s3://" + TEST_BUCKET)
            .setProperty(PropertyKey.S3A_ACCESS_KEY, mS3Proxy.getAccessKey())
            .setProperty(PropertyKey.S3A_SECRET_KEY, mS3Proxy.getSecretKey())
            .setStartCluster(false)
            .build();

  private AmazonS3 mS3Client = null;



  @Before
  public void before() throws Exception {
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
    mS3Client.createBucket(TEST_BUCKET);

    System.out.println(mS3Client.listBuckets());

    mLocalAlluxioClusterResource.start();
    mLocalAlluxioCluster = mLocalAlluxioClusterResource.get();
    mLocalAlluxioJobCluster = new LocalAlluxioJobCluster();
    mLocalAlluxioJobCluster.start();
    mFileSystem = mLocalAlluxioCluster.getClient();
    mJobMaster = mLocalAlluxioJobCluster.getMaster().getJobMaster();
    mJobShell = new alluxio.cli.job.JobShell(Configuration.global());
    mFsShell = new FileSystemShell(Configuration.global());
  }

  @After
  public void after() throws Exception {
    mS3Client = null;
    if (mFsShell != null) {
      mFsShell.close();
    }
    if (mLocalAlluxioJobCluster != null) {
      mLocalAlluxioJobCluster.stop();
    }
    if (mJobShell != null) {
      mJobShell.close();
    }
  }

  @Test
  public void test() {
    String str = "...... Task group 0 has been submitted successfully. ......";
    Pattern pattern = Pattern.compile("Task group (\\d+)");
    Matcher matcher = pattern.matcher(str);
    if (matcher.find()) {
      String match = matcher.group(1); // 获取匹配到的数字字符串
      int num = Integer.parseInt(match); // 将字符串转换为 int 类型
      System.out.println(num);
    }
  }

  // The main idea of this test is start an async loadMetadata task and get its status
  // Totally three status were tested, RUNNING, CANCELED, SUCCESSES
  @Test
  public void loadMetadataTestV2get() throws IOException, AlluxioException {
    // the cancel dir should be big enough
    for (int i = 0; i < 100; i++) {
      mS3Client.putObject(TEST_BUCKET, TEST_FILE + i, TEST_CONTENT);
    }
    mOutput.reset();
    AlluxioURI uriDir = new AlluxioURI("/" );
    // To avoid the loadMetadata blocked until finish
    // -a/--async param will disable loadMetadata tell anything include task group id, so can't obtain group id from output here
    CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
      mFsShell.run("loadMetadata", "-v2", "-R", uriDir.toString());
      return null;
    });
    FileSystemShell anotherFsShell = new FileSystemShell(Configuration.global());
    // Running
    anotherFsShell.run("loadMetadata", "-v2", "-o", "get", "-id", "0");
    assertTrue(mOutput.toString().contains("State: RUNNING"));
    // Cancel success
    anotherFsShell.run("loadMetadata", "-v2", "-o", "cancel", "-id", "0");
    assertTrue(mOutput.toString().contains("Task group 0 cancelled"));
    // Get cancel
    anotherFsShell.run("loadMetadata", "-v2", "-o", "get", "-id", "0");
    assertTrue(mOutput.toString().contains("State: CANCELED"));
    mOutput.reset();
    mFsShell.run("loadMetadata", "-v2", "-R", uriDir.toString());
    // clean the mOutput stream
    mOutput.reset();
    // start a new loadMetadata task for SUCCEEDED test
    anotherFsShell.run("loadMetadata", "-v2", "-o", "get", "-id", "1");
    assertTrue(mOutput.toString().contains("State: SUCCEEDED"));
  }

  // The main idea of this test is start an async loadMetadata task and cancel it when it's running
  // I think this is difficult to fix...
  @Test
  public void loadMetadataTestV2cancel() {
    for (int i = 0; i < 100; i++) {
      mS3Client.putObject(TEST_BUCKET, TEST_FILE + i, TEST_CONTENT);
    }
    mOutput.reset();
    AlluxioURI uriDir = new AlluxioURI("/" );
    CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
      mFsShell.run("loadMetadata", "-v2", "-R", "-a", uriDir.toString());
      return null;
    });
    String id;
    Pattern pattern = Pattern.compile("Task group (\\d+)");
    Matcher matcher = pattern.matcher(mOutput.toString());
    while(!matcher.find()) {
      matcher = pattern.matcher(mOutput.toString());
    }
    id = matcher.group(1);
    FileSystemShell anotherFsShell = new FileSystemShell(Configuration.global());

    anotherFsShell.run("loadMetadata", "-v2", "-o", "cancel", "-id", id);
    assertTrue(mOutput.toString().contains(String.format("Task group %s cancelled", id)));
    mOutput.reset();
    anotherFsShell.run("loadMetadata", "-v2", "-o", "cancel", "-id", id);
    assertTrue(mOutput.toString().contains(String.format("Task %s not found or has already been canceled", id)));
  }

  @Test
  public void loadMetadataTestV2R() throws IOException, AlluxioException {
    // dir number
    int dirCount = 2;
    // Child number for one dir
    int fileCount = 100;
    for (int dirIndex = 0; dirIndex < dirCount; dirIndex++) {
      for (int fileIndex = 0; fileIndex < fileCount; fileIndex++) {
        mS3Client.putObject(TEST_BUCKET, "test" + dirIndex + "/" + fileIndex, TEST_CONTENT);
      }
    }
    AlluxioURI uriDir = new AlluxioURI("/" );

    mFsShell.run("loadMetadata", "-v2", "-a", uriDir.toString());
    assertTrue(mOutput.toString().contains(String.format("Success op count={[CREATE:%d]}", dirCount)));
    mOutput.reset();
    mFsShell.run("loadMetadata", "-v2", "-R", "-a", uriDir.toString());
    assertTrue(mOutput.toString().contains(String.format("Success op count={[CREATE:%d]}", dirCount * fileCount)));
  }

  @Test
  public void loadMetadataTestV2HeavyLoad() throws IOException, AlluxioException {
    int fileCount = 10000;
    for (int i = 0; i < fileCount; i++) {
      mS3Client.putObject(TEST_BUCKET, TEST_FILE + i, TEST_CONTENT);
    }
    mOutput.reset();
    AlluxioURI uriDir = new AlluxioURI("/" );
    mFsShell.run("loadMetadata", "-v2", "-R", "-a", uriDir.toString());
    assertTrue(mOutput.toString().contains("State: SUCCEEDED"));
    assertTrue(mOutput.toString().contains(String.format("Success op count={[CREATE:%d]}", fileCount)));
  }
}
