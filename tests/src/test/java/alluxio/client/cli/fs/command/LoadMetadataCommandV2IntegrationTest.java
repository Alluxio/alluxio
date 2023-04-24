package alluxio.client.cli.fs.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.SystemErrRule;
import alluxio.SystemOutRule;
import alluxio.cli.fs.FileSystemShell;
import alluxio.cli.job.JobShell;
import alluxio.client.cli.fs.AbstractFileSystemShellTest;
import alluxio.client.cli.fs.AbstractShellIntegrationTest;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.concurrent.jsr.CompletableFuture;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateFilePOptions;
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
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.io.IOUtils;
import org.gaul.s3proxy.junit.S3ProxyRule;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;
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
    System.out.println("before before");
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
    System.out.println("after before");
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

    System.out.println("testing");
  }

  @Test
  public void loadMetadataTestV2Test() throws IOException, AlluxioException {
    mS3Client.putObject(TEST_BUCKET, TEST_FILE, TEST_CONTENT);
    AlluxioURI uriDir = new AlluxioURI("/" + TEST_FILE);
    AlluxioURI uriA = new AlluxioURI(uriDir + "/" + TEST_CONTENT);
    try {
      URIStatus statusBeforeA = mFileSystem.getStatus(new AlluxioURI("/"));
      System.out.println("StatusBefore: " + statusBeforeA);
    } catch (Exception e) {
      System.out.println("getting A: " + e);
    }
    mFsShell.run("loadMetadata", "-v2", "-R", uriDir.toString());
    System.out.println("-------------");
    mFsShell.run("ls", "-R", "/");
    System.out.println("-------------");
    S3Object s = mS3Client.getObject(TEST_BUCKET, TEST_FILE);
    ObjectListing o = mS3Client.listObjects(TEST_BUCKET);
    System.out.println("ObjectListing: " + o);
    System.out.println("s: " + s);
    // Use LoadMetadataPType.NEVER to avoid loading metadata during get file status.
    GetStatusPOptions getStatusPOptions =
        GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER).build();
    // Check testFileA's metadata.
    URIStatus statusAfterA = mFileSystem.getStatus(new AlluxioURI("/" + TEST_FILE), getStatusPOptions);
    // System.out.println("StatusBefore: " + statusBeforeA);
    System.out.println("StatusAfter: " + statusAfterA);
    // assertEquals(statusBeforeA.getFileInfo().getName(), statusAfterA.getFileInfo().getName());
    // assertEquals(statusBeforeA.getFileInfo().getLength(), statusAfterA.getFileInfo().getLength());
    // Check testFileB's metadata.
  }

  @Test
  public void loadMetadataTestV2Dir() throws IOException, AlluxioException {
    String dirPath = "/testRoot/layer1/layer2/layer3/";
    String filePathA = PathUtils.concatPath(dirPath, "testFileA");
    String filePathB = PathUtils.concatPath(dirPath, "testFileB");
    FileSystemTestUtils
        .createByteFile(mFileSystem, filePathA, WritePType.CACHE_THROUGH, 10);
    FileSystemTestUtils
        .createByteFile(mFileSystem, filePathB, WritePType.CACHE_THROUGH, 30);
    AlluxioURI uriDir = new AlluxioURI(dirPath);
    AlluxioURI uriA = new AlluxioURI(filePathA);
    AlluxioURI uriB = new AlluxioURI(filePathB);
    URIStatus statusBeforeA = mFileSystem.getStatus(uriA);
    URIStatus statusBeforeB = mFileSystem.getStatus(uriB);
    // Delete layer3 directory metadata recursively.
    DeletePOptions deletePOptions =
        DeletePOptions.newBuilder().setAlluxioOnly(true).setRecursive(true).build();
    mFileSystem.delete(uriDir, deletePOptions);
    // Load metadata from ufs.
    mFsShell.run("loadMetadata", "-v2", dirPath);
    // Use LoadMetadataPType.NEVER to avoid loading metadata during get file status.
    GetStatusPOptions getStatusPOptions =
        GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER).build();
    // Check testFileA's metadata.
    URIStatus statusAfterA = mFileSystem.getStatus(uriA, getStatusPOptions);
    assertEquals(statusBeforeA.getFileInfo().getName(), statusAfterA.getFileInfo().getName());
    assertEquals(statusBeforeA.getFileInfo().getLength(), statusAfterA.getFileInfo().getLength());
    // Check testFileB's metadata.
    URIStatus statusAfterB = mFileSystem.getStatus(uriB, getStatusPOptions);
    assertEquals(statusBeforeB.getFileInfo().getName(), statusAfterB.getFileInfo().getName());
    assertEquals(statusBeforeB.getFileInfo().getLength(), statusAfterB.getFileInfo().getLength());
  }

  // The main idea of this test is start an async loadMetadata task and get its status
  @Test
  public void loadMetadataTestV2get() throws IOException, AlluxioException {
    // the cancel dir should be big enough
    for (int i = 0; i < 100; i++) {
      mS3Client.putObject(TEST_BUCKET, TEST_FILE + i, TEST_CONTENT);
    }
    AlluxioURI uriDir = new AlluxioURI("/" );
    CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
      mFsShell.run("loadMetadata", "-v2", "-R", uriDir.toString());
      return null;
    });
    FileSystemShell anotherFsShell = new FileSystemShell(Configuration.global());
    anotherFsShell.run("loadMetadata", "-v2", "-o", "get", "-id", "0");
    assertTrue(mOutput.toString().contains("State: RUNNING"));
    anotherFsShell.run("loadMetadata", "-v2", "-o", "cancel", "-id", "0");
    assertTrue(mOutput.toString().contains("Task group 0 cancelled"));
    anotherFsShell.run("loadMetadata", "-v2", "-o", "get", "-id", "0");
    assertTrue(mOutput.toString().contains("State: CANCELED"));
    mFsShell.run("loadMetadata", "-v2", "-R", uriDir.toString());
    mOutput.reset();
    anotherFsShell.run("loadMetadata", "-v2", "-o", "get", "-id", "1");
    assertTrue(mOutput.toString().contains("State: SUCCEEDED"));
  }

  // The main idea of this test is start an async loadMetadata task and cancel it when it's running
  @Test
  public void loadMetadataTestV2cancel() throws IOException, InterruptedException, AlluxioException {
    // the cancel dir should be big enough
    for (int i = 0; i < 100; i++) {
      mS3Client.putObject(TEST_BUCKET, TEST_FILE + i, TEST_CONTENT);
    }
    // AlluxioURI uriDir = new AlluxioURI("/" + TEST_FILE);
    AlluxioURI uriDir = new AlluxioURI("/" );
    CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
      mFsShell.run("loadMetadata", "-v2", "-R", "-a", uriDir.toString());
      return null;
    });
    // mFsShell.run("loadMetadata", "-v2", "-R", "-a", uriDir.toString());
    // this is task group id that comes from mFsShell loadMetadata
    int id = 0;
    Pattern pattern = Pattern.compile("Task group id: (\\d+)");
    Matcher matcher = pattern.matcher(mOutput.toString());
    // if (matcher.find()) {
    //   id = Integer.parseInt(matcher.group(1));
    // } else {
    //   // assertTrue(false);
    // }
    // mOutput.flush();
    FileSystemShell anotherFsShell = new FileSystemShell(Configuration.global());
    // anotherFsShell.run("loadMetadata", "-v2", "-o cancel", "-id", matcher.group(1));

    anotherFsShell.run("loadMetadata", "-v2", "-o", "cancel", "-id", "0");
    assertTrue(mOutput.toString().contains("Task group 0 cancelled"));
    // assertTrue(mOutput.toString().contains("Load Metadata Canceled"));
    // assertTrue(mOutput.toString().contains(String.format("Task gourp %d cancelled", id)));
    mOutput.flush();
    anotherFsShell.run("loadMetadata", "-v2", "-o", "cancel", "-id", "0");
    assertTrue(mOutput.toString().contains(String.format("Task %d not found or has already been canceled", 0)));
  }

  // I think here each Param should have a Test
  @Test
  public void loadMetadataTestV2R() throws IOException, AlluxioException {

  }

  // This seems not supported for v2
  @Test
  public void loadMetadataTestV2F() throws IOException, AlluxioException {

  }

  @Test
  public void loadMetadataTestV2() throws IOException, AlluxioException {

  }

  // heavy load success
  // cancel
  // -R
  // get..?
  // id
}
