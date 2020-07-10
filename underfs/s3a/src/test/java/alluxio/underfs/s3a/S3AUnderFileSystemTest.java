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

package alluxio.underfs.s3a;

import alluxio.AlluxioURI;
import alluxio.ConfigurationRule;
import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.underfs.ObjectUnderFileSystem;
import alluxio.underfs.UfsMode;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.DeleteOptions;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

/**
 * Unit tests for the {@link S3AUnderFileSystem}.
 */
public class S3AUnderFileSystemTest {
  private static final String PATH = "path";
  private static final String SRC = "src";
  private static final String DST = "dst";
  private static  InstancedConfiguration sConf = ConfigurationTestUtils.defaults();

  private static final String BUCKET_NAME = "bucket";
  private static final String DEFAULT_OWNER = "";
  private static final short DEFAULT_MODE = 0700;

  private S3AUnderFileSystem mS3UnderFileSystem;
  private AmazonS3Client mClient;
  private ListeningExecutorService mExecutor;
  private TransferManager mManager;

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws InterruptedException, AmazonClientException {
    mClient = Mockito.mock(AmazonS3Client.class);
    mExecutor = Mockito.mock(ListeningExecutorService.class);
    mManager = Mockito.mock(TransferManager.class);
    mS3UnderFileSystem =
        new S3AUnderFileSystem(new AlluxioURI("s3a://" + BUCKET_NAME), mClient, BUCKET_NAME,
            mExecutor, mManager, UnderFileSystemConfiguration.defaults(sConf), false);
  }

  @Test
  public void deleteNonRecursiveOnAmazonClientException() throws IOException {
    Mockito.when(mClient.listObjectsV2(Matchers.any(ListObjectsV2Request.class)))
        .thenThrow(AmazonClientException.class);

    mThrown.expect(IOException.class);
    mS3UnderFileSystem.deleteDirectory(PATH, DeleteOptions.defaults().setRecursive(false));
  }

  @Test
  public void deleteRecursiveOnAmazonClientException() throws IOException {
    Mockito.when(mClient.listObjectsV2(Matchers.any(ListObjectsV2Request.class)))
        .thenThrow(AmazonClientException.class);

    mThrown.expect(IOException.class);
    mS3UnderFileSystem.deleteDirectory(PATH, DeleteOptions.defaults().setRecursive(true));
  }

  @Test
  public void isFile404() throws IOException {
    AmazonServiceException e = new AmazonServiceException("");
    e.setStatusCode(404);
    Mockito.when(mClient.getObjectMetadata(Matchers.anyString(), Matchers.anyString()))
        .thenThrow(e);

    Assert.assertFalse(mS3UnderFileSystem.isFile(SRC));
  }

  @Test
  public void isFileException() throws IOException {
    AmazonServiceException e = new AmazonServiceException("");
    e.setStatusCode(403);
    Mockito.when(mClient.getObjectMetadata(Matchers.anyString(), Matchers.anyString()))
        .thenThrow(e);

    mThrown.expect(IOException.class);
    Assert.assertFalse(mS3UnderFileSystem.isFile(SRC));
  }

  @Test
  public void renameOnAmazonClientException() throws IOException {
    Mockito.when(mClient.getObjectMetadata(Matchers.anyString(), Matchers.anyString()))
        .thenThrow(AmazonClientException.class);

    mThrown.expect(IOException.class);
    mS3UnderFileSystem.renameFile(SRC, DST);
  }

  @Test
  public void createCredentialsFromConf() throws Exception {
    Map<PropertyKey, String> conf = new HashMap<>();
    conf.put(PropertyKey.S3A_ACCESS_KEY, "key1");
    conf.put(PropertyKey.S3A_SECRET_KEY, "key2");
    try (Closeable c = new ConfigurationRule(conf, sConf).toResource()) {
      UnderFileSystemConfiguration ufsConf = UnderFileSystemConfiguration.defaults(sConf);
      AWSCredentialsProvider credentialsProvider =
          S3AUnderFileSystem.createAwsCredentialsProvider(ufsConf);
      Assert.assertEquals("key1", credentialsProvider.getCredentials().getAWSAccessKeyId());
      Assert.assertEquals("key2", credentialsProvider.getCredentials().getAWSSecretKey());
      Assert.assertTrue(credentialsProvider instanceof AWSStaticCredentialsProvider);
    }
  }

  @Test
  public void createCredentialsFromDefault() throws Exception {
    // Unset AWS properties if present
    Map<PropertyKey, String> conf = new HashMap<>();
    conf.put(PropertyKey.S3A_ACCESS_KEY, null);
    conf.put(PropertyKey.S3A_SECRET_KEY, null);
    try (Closeable c = new ConfigurationRule(conf, sConf).toResource()) {
      UnderFileSystemConfiguration ufsConf = UnderFileSystemConfiguration.defaults(sConf);
      AWSCredentialsProvider credentialsProvider =
          S3AUnderFileSystem.createAwsCredentialsProvider(ufsConf);
      Assert.assertTrue(credentialsProvider instanceof DefaultAWSCredentialsProviderChain);
    }
  }

  @Test
  public void getPermissionsCached() throws Exception {
    Mockito.when(mClient.getS3AccountOwner()).thenReturn(new Owner("0", "test"));
    Mockito.when(mClient.getBucketAcl(Mockito.anyString())).thenReturn(new AccessControlList());
    mS3UnderFileSystem.getPermissions();
    mS3UnderFileSystem.getPermissions();
    Mockito.verify(mClient).getS3AccountOwner();
    Mockito.verify(mClient).getBucketAcl(Mockito.anyString());
  }

  @Test
  public void getPermissionsDefault() throws Exception {
    Mockito.when(mClient.getS3AccountOwner()).thenThrow(AmazonClientException.class);
    ObjectUnderFileSystem.ObjectPermissions permissions = mS3UnderFileSystem.getPermissions();
    Assert.assertEquals(DEFAULT_OWNER, permissions.getGroup());
    Assert.assertEquals(DEFAULT_OWNER, permissions.getOwner());
    Assert.assertEquals(DEFAULT_MODE, permissions.getMode());
  }

  @Test
  public void getPermissionsWithMapping() throws Exception {
    Map<PropertyKey, String> conf = new HashMap<>();
    conf.put(PropertyKey.UNDERFS_S3_OWNER_ID_TO_USERNAME_MAPPING, "111=altname");
    try (Closeable c = new ConfigurationRule(conf, sConf).toResource()) {
      UnderFileSystemConfiguration ufsConf = UnderFileSystemConfiguration.defaults(sConf);
      mS3UnderFileSystem =
              new S3AUnderFileSystem(new AlluxioURI("s3a://" + BUCKET_NAME), mClient, BUCKET_NAME,
                      mExecutor, mManager, UnderFileSystemConfiguration.defaults(sConf), false);
    }

    Mockito.when(mClient.getS3AccountOwner()).thenReturn(new Owner("111", "test"));
    Mockito.when(mClient.getBucketAcl(Mockito.anyString())).thenReturn(new AccessControlList());
    ObjectUnderFileSystem.ObjectPermissions permissions = mS3UnderFileSystem.getPermissions();

    Assert.assertEquals("altname", permissions.getOwner());
    Assert.assertEquals("altname", permissions.getGroup());
    Assert.assertEquals(0, permissions.getMode());
  }

  @Test
  public void getPermissionsNoMapping() throws Exception {
    Map<PropertyKey, String> conf = new HashMap<>();
    conf.put(PropertyKey.UNDERFS_S3_OWNER_ID_TO_USERNAME_MAPPING, "111=userid");
    try (Closeable c = new ConfigurationRule(conf, sConf).toResource()) {
      UnderFileSystemConfiguration ufsConf = UnderFileSystemConfiguration.defaults(sConf);
      mS3UnderFileSystem =
              new S3AUnderFileSystem(new AlluxioURI("s3a://" + BUCKET_NAME), mClient, BUCKET_NAME,
                      mExecutor, mManager, UnderFileSystemConfiguration.defaults(sConf), false);
    }

    Mockito.when(mClient.getS3AccountOwner()).thenReturn(new Owner("0", "test"));
    Mockito.when(mClient.getBucketAcl(Mockito.anyString())).thenReturn(new AccessControlList());
    ObjectUnderFileSystem.ObjectPermissions permissions = mS3UnderFileSystem.getPermissions();

    Assert.assertEquals("test", permissions.getOwner());
    Assert.assertEquals("test", permissions.getGroup());
    Assert.assertEquals(0, permissions.getMode());
  }

  @Test
  public void getOperationMode() throws Exception {
    Map<String, UfsMode> physicalUfsState = new Hashtable<>();
    // Check default
    Assert.assertEquals(UfsMode.READ_WRITE,
        mS3UnderFileSystem.getOperationMode(physicalUfsState));
    physicalUfsState.put(new AlluxioURI("swift://" + BUCKET_NAME).getRootPath(),
        UfsMode.NO_ACCESS);
    Assert.assertEquals(UfsMode.READ_WRITE,
        mS3UnderFileSystem.getOperationMode(physicalUfsState));
    // Check setting NO_ACCESS mode
    physicalUfsState.put(new AlluxioURI("s3a://" + BUCKET_NAME).getRootPath(),
        UfsMode.NO_ACCESS);
    Assert.assertEquals(UfsMode.NO_ACCESS,
        mS3UnderFileSystem.getOperationMode(physicalUfsState));
    // Check setting READ_ONLY mode
    physicalUfsState.put(new AlluxioURI("s3a://" + BUCKET_NAME).getRootPath(),
        UfsMode.READ_ONLY);
    Assert.assertEquals(UfsMode.READ_ONLY,
        mS3UnderFileSystem.getOperationMode(physicalUfsState));
    // Check setting READ_WRITE mode
    physicalUfsState.put(new AlluxioURI("s3a://" + BUCKET_NAME).getRootPath(),
        UfsMode.READ_WRITE);
    Assert.assertEquals(UfsMode.READ_WRITE,
        mS3UnderFileSystem.getOperationMode(physicalUfsState));
  }

  @Test
  public void stripPrefixIfPresent() throws Exception {
    Assert.assertEquals("", mS3UnderFileSystem.stripPrefixIfPresent("s3a://" + BUCKET_NAME));
    Assert.assertEquals("", mS3UnderFileSystem.stripPrefixIfPresent("s3a://" + BUCKET_NAME + "/"));
    Assert.assertEquals("test/",
        mS3UnderFileSystem.stripPrefixIfPresent("s3a://" + BUCKET_NAME + "/test/"));
    Assert.assertEquals("test", mS3UnderFileSystem.stripPrefixIfPresent("test"));
    Assert.assertEquals("test/", mS3UnderFileSystem.stripPrefixIfPresent("test/"));
    Assert.assertEquals("test/", mS3UnderFileSystem.stripPrefixIfPresent("/test/"));
    Assert.assertEquals("test", mS3UnderFileSystem.stripPrefixIfPresent("/test"));
    Assert.assertEquals("", mS3UnderFileSystem.stripPrefixIfPresent(""));
    Assert.assertEquals("", mS3UnderFileSystem.stripPrefixIfPresent("/"));
  }
}
