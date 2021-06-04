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

package alluxio.underfs.gcs.v2;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.ObjectUnderFileSystem;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.ModeUtils;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;

import com.google.api.client.util.Base64;
import com.google.api.gax.paging.Page;
import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.Lists;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * GCS FS {@link UnderFileSystem} implementation based on the Google cloud storage library.
 */
@ThreadSafe
public class GCSV2UnderFileSystem extends ObjectUnderFileSystem {
  // TODO(lu) StorageException has isRetryable() method, can help handle retry
  private static final Logger LOG = LoggerFactory.getLogger(GCSV2UnderFileSystem.class);

  /** Static hash for a directory's empty contents. */
  private static final String DIR_HASH = Base64.encodeBase64String(DigestUtils.md5(new byte[0]));

  /** Google cloud storage client. */
  private final Storage mStorageClient;

  /** Bucket name of user's configured Alluxio bucket. */
  private final String mBucketName;

  /**
   * Constructs a new instance of {@link GCSV2UnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @return the created {@link GCSV2UnderFileSystem} instance
   */
  public static GCSV2UnderFileSystem createInstance(AlluxioURI uri,
      UnderFileSystemConfiguration conf) throws IOException {
    String bucketName = UnderFileSystemUtils.getBucketName(uri);
    GoogleCredentials credentials;
    if (conf.isSet(PropertyKey.GCS_CREDENTIAL_PATH)) {
      String credsPath = conf.get(PropertyKey.GCS_CREDENTIAL_PATH);
      credentials = GoogleCredentials
          .fromStream(new FileInputStream(credsPath))
          .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
      LOG.info("Created GCSV2UnderFileSystem with credentials in {}", credsPath);
    } else {
      // The environment variable GOOGLE_APPLICATION_CREDENTIALS is set
      // or the application is running in Google App engine or compute engine
      credentials = GoogleCredentials.getApplicationDefault();
      LOG.info("Created GCSV2UnderFileSystem with default Google application credentials");
    }
    Storage storage = StorageOptions.newBuilder().setRetrySettings(
        RetrySettings.newBuilder()
            .setInitialRetryDelay(
                Duration.ofMillis(conf.getInt(PropertyKey.UNDERFS_GCS_RETRY_INITIAL_DELAY_MS)))
            .setMaxRetryDelay(
                Duration.ofMillis(conf.getInt(PropertyKey.UNDERFS_GCS_RETRY_MAX_DELAY_MS)))
            .setRetryDelayMultiplier(
                conf.getInt(PropertyKey.UNDERFS_GCS_RETRY_DELAY_MULTIPLIER))
            .setMaxAttempts(conf.getInt(PropertyKey.UNDERFS_GCS_RETRY_MAX))
            .setTotalTimeout(
                Duration.ofMillis(conf.getInt(PropertyKey.UNDERFS_GCS_RETRY_TOTAL_DURATION_MS)))
            .setJittered(conf.getBoolean(PropertyKey.UNDERFS_GCS_RETRY_JITTER))
            .build())
        .setCredentials(credentials).build().getService();
    return new GCSV2UnderFileSystem(uri, storage, bucketName, conf);
  }

  /**
   * Constructor for {@link GCSV2UnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param storageClient the Google cloud storage client
   * @param bucketName bucket name of user's configured Alluxio bucket
   * @param conf configuration for this UFS
   */
  protected GCSV2UnderFileSystem(AlluxioURI uri, Storage storageClient,
      String bucketName, UnderFileSystemConfiguration conf) {
    super(uri, conf);
    mStorageClient = storageClient;
    mBucketName = bucketName;
  }

  @Override
  public String getUnderFSType() {
    return "gcs";
  }

  // Setting GCS owner via Alluxio is not supported yet. This is a no-op.
  @Override
  public void setOwner(String path, String user, String group) {}

  // Setting GCS mode via Alluxio is not supported yet. This is a no-op.
  @Override
  public void setMode(String path, short mode) {}

  @Override
  protected boolean copyObject(String src, String dst) {
    LOG.debug("Copying {} to {}", src, dst);
    Storage.CopyRequest request = Storage.CopyRequest.newBuilder()
        .setSource(BlobId.of(mBucketName, src))
        .setTarget(BlobId.of(mBucketName, dst))
        .build();
    try {
      Blob blob = mStorageClient.copy(request).getResult();
      if (blob != null) {
        return true;
      }
    } catch (StorageException e) {
      LOG.error("Failed to copy file {} to {}", src, dst, e);
    }
    return false;
  }

  @Override
  public boolean createEmptyObject(String key) {
    try {
      BlobId blobId = BlobId.of(mBucketName, key);
      BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
      Blob blob = mStorageClient.create(blobInfo);
      if (blob == null) {
        LOG.error("Failed to create object {}", key);
        return false;
      }
    } catch (StorageException e) {
      LOG.error("Failed to create object: {}", key, e);
      return false;
    }
    return true;
  }

  @Override
  protected OutputStream createObject(String key) throws IOException {
    return new GCSV2OutputStream(mBucketName, key, mStorageClient);
  }

  @Override
  protected boolean deleteObject(String key) {
    // TODO(lu) Google cloud API supports bunch delete
    BlobId blobId = BlobId.of(mBucketName, key);
    try {
      if (!mStorageClient.delete(blobId)) {
        LOG.error("Failed to delete object {}", key);
        return false;
      }
    } catch (StorageException e) {
      LOG.error("Failed to delete {}", key, e);
      return false;
    }
    return true;
  }

  @Override
  protected String getFolderSuffix() {
    return mUfsConf.get(PropertyKey.UNDERFS_GCS_DIRECTORY_SUFFIX);
  }

  @Override
  protected ObjectListingChunk getObjectListingChunk(String key, boolean recursive)
      throws IOException {
    key = PathUtils.normalizePath(key, PATH_SEPARATOR);
    // In case key is root (empty string) do not normalize prefix
    key = key.equals(PATH_SEPARATOR) ? "" : key;
    Page<Blob> blobPage;
    try {
      if (recursive) {
        blobPage = mStorageClient.list(mBucketName, Storage.BlobListOption.prefix(key));
      } else {
        blobPage = mStorageClient.list(mBucketName, Storage.BlobListOption.prefix(key),
            Storage.BlobListOption.currentDirectory());
      }
    } catch (StorageException e) {
      LOG.error("Failed to get object listing result of {}", key, e);
      throw new IOException(e);
    }
    if (blobPage != null && blobPage.getValues().iterator().hasNext()) {
      return new GCSObjectListingChunk(blobPage);
    }
    return null;
  }

  /**
   * Wrapper over GCS.
   */
  private final class GCSObjectListingChunk implements ObjectListingChunk {
    final Page<Blob> mBlobPage;

    /**
     * Creates an instance of {@link GCSObjectListingChunk}.
     *
     * @param blobPage blob page
     */
    GCSObjectListingChunk(Page<Blob> blobPage) {
      mBlobPage = blobPage;
    }

    @Override
    public ObjectStatus[] getObjectStatuses() {
      Iterator<Blob> blobs = mBlobPage.getValues().iterator();
      List<Blob> blobList = new ArrayList<>();
      while (blobs.hasNext()) {
        blobList.add(blobs.next());
      }
      ObjectStatus[] res = new ObjectStatus[blobList.size()];
      for (int i = 0; i < res.length; i++) {
        Blob blob = blobList.get(i);
        res[i] = getBlobStatus(blob);
      }
      return res;
    }

    @Override
    public String[] getCommonPrefixes() {
      return new String[0];
    }

    @Override
    public ObjectListingChunk getNextChunk() {
      if (mBlobPage.hasNextPage()) {
        return new GCSObjectListingChunk(mBlobPage.getNextPage());
      }
      return null;
    }
  }

  @Override
  protected ObjectStatus getObjectStatus(String key) throws IOException {
    try {
      BlobId info = BlobId.of(mBucketName, key);
      Blob blob = mStorageClient.get(info);
      if (blob == null) {
        // file not found, possible for exists calls
        return null;
      }
      return getBlobStatus(blob);
    } catch (StorageException e) {
      if (e.getCode() == 404) { // file not found, possible for exists calls
        return null;
      }
      throw new IOException(String
          .format("Failed to get object status of %s, %s", key, mBucketName), e);
    }
  }

  @Override
  protected ObjectPermissions getPermissions() {
    // TODO(lu) inherit acl
    return new ObjectPermissions("", "",
        ModeUtils.getUMask(mUfsConf.get(PropertyKey.UNDERFS_GCS_DEFAULT_MODE)).toShort());
  }

  @Override
  protected String getRootKey() {
    return Constants.HEADER_GCS + mBucketName;
  }

  @Override
  protected InputStream openObject(String key, OpenOptions options,
      RetryPolicy retryPolicy) throws IOException {
    return new GCSV2InputStream(mBucketName, key, mStorageClient, options.getOffset());
  }

  /**
   * Gets the blob status.
   *
   * @param blob the blob to get status from
   * @return the blob status
   */
  private ObjectStatus getBlobStatus(Blob blob) {
    long time = blob.getUpdateTime() != null ? blob.getUpdateTime()
        : blob.getCreateTime() != null ? blob.getCreateTime() : -1;
    return new ObjectStatus(blob.getName(), blob.getMd5() == null ? DIR_HASH : blob.getMd5(),
        blob.getSize(), time);
  }
}
