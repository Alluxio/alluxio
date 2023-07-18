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

package alluxio.underfs.tos;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.PositionReader;
import alluxio.conf.PropertyKey;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.ObjectUnderFileSystem;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import com.volcengine.tos.TOSV2;
import com.volcengine.tos.TOSV2ClientBuilder;
import com.volcengine.tos.TosClientException;
import com.volcengine.tos.TosServerException;
import com.volcengine.tos.model.object.CopyObjectV2Input;
import com.volcengine.tos.model.object.DeleteMultiObjectsV2Input;
import com.volcengine.tos.model.object.DeleteObjectInput;
import com.volcengine.tos.model.object.HeadObjectV2Input;
import com.volcengine.tos.model.object.HeadObjectV2Output;
import com.volcengine.tos.model.object.ListObjectsType2Input;
import com.volcengine.tos.model.object.ListObjectsType2Output;
import com.volcengine.tos.model.object.ListedCommonPrefix;
import com.volcengine.tos.model.object.ListedObjectV2;
import com.volcengine.tos.model.object.ObjectMetaRequestOptions;
import com.volcengine.tos.model.object.ObjectTobeDeleted;
import com.volcengine.tos.model.object.PutObjectInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;

/**
 * TOS (Tinder Object Storage) {@link UnderFileSystem} implementation.
 */
@ThreadSafe
public class TOSUnderFileSystem extends ObjectUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(TOSUnderFileSystem.class);

  /** Suffix for an empty file to flag it as a directory. */
  private static final String FOLDER_SUFFIX = "/";

  /** TOS client. */
  private final TOSV2 mClient;

  /** Bucket name of user's configured Alluxio bucket. */
  private final String mBucketName;

  /**
   * Constructs a new instance of {@link TOSUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @return the created {@link TOSUnderFileSystem} instance
   */
  public static TOSUnderFileSystem createInstance(AlluxioURI uri,
      UnderFileSystemConfiguration conf) {
    String bucketName = UnderFileSystemUtils.getBucketName(uri);
    Preconditions.checkArgument(conf.isSet(PropertyKey.TOS_ACCESS_KEY),
        "Property %s is required to connect to TOS", PropertyKey.TOS_ACCESS_KEY);
    Preconditions.checkArgument(conf.isSet(PropertyKey.TOS_SECRET_KEY),
        "Property %s is required to connect to TOS", PropertyKey.TOS_SECRET_KEY);
    Preconditions.checkArgument(conf.isSet(PropertyKey.TOS_ENDPOINT),
        "Property %s is required to connect to TOS", PropertyKey.TOS_ENDPOINT);
    Preconditions.checkArgument(conf.isSet(PropertyKey.TOS_REGION),
        "Property %s is required to connect to TOS", PropertyKey.TOS_REGION);
    String accessKey = conf.getString(PropertyKey.TOS_ACCESS_KEY);
    String secretKey = conf.getString(PropertyKey.TOS_SECRET_KEY);
    String endpoint = conf.getString(PropertyKey.TOS_ENDPOINT);
    String region = conf.getString(PropertyKey.TOS_REGION);

    TOSV2 client = new TOSV2ClientBuilder().build(region, endpoint, accessKey, secretKey);

    return new TOSUnderFileSystem(uri, client, bucketName, conf);
  }

  /**
   * Constructor for {@link TOSUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param client TOS client
   * @param bucketName bucket name of user's configured Alluxio bucket
   * @param conf configuration for this UFS
   */
  protected TOSUnderFileSystem(AlluxioURI uri, TOSV2 client, String bucketName,
                               UnderFileSystemConfiguration conf) {
    super(uri, conf);
    mClient = client;
    mBucketName = bucketName;
  }

  @Override
  public String getUnderFSType() {
    return "tos";
  }

  // No ACL integration currently, no-op
  @Override
  public void setOwner(String path, String user, String group) {}

  // No ACL integration currently, no-op
  @Override
  public void setMode(String path, short mode) {}

  @Override
  protected boolean copyObject(String src, String dst) {
    try {
      LOG.debug("Copying {} to {}", src, dst);
      mClient.copyObject(new CopyObjectV2Input().setBucket(mBucketName)
          .setKey(src).setSrcBucket(mBucketName).setSrcKey(dst));
      return true;
    } catch (TosClientException e) {
      LOG.error("Failed to rename file {} to {}", src, dst, e);
      if (e.getCause()  != null) {
        e.getCause().printStackTrace();
      }
      return false;
    }
    catch (TosServerException e) {
      LOG.error("Failed to rename file {} to {}", src, dst, e);
    }
    return false;
  }

  @Override
  public boolean createEmptyObject(String key) {
    try {
      ObjectMetaRequestOptions options = new ObjectMetaRequestOptions();
      options.setContentLength(0);
      mClient.putObject(new PutObjectInput().setBucket(mBucketName).setKey(key)
          .setOptions(options).setContent(new ByteArrayInputStream(new byte[0])));
      return true;
    } catch (TosClientException e) {
      LOG.error("Failed to create object: {}", key, e);
      return false;
    } catch (TosServerException e) {
      LOG.error("Failed to create object: {}", key, e);
      return false;
    }
  }

  @Override
  protected OutputStream createObject(String key) throws IOException {
    return new TOSOutputStream(mBucketName, key, mClient,
        mUfsConf.getList(PropertyKey.TMP_DIRS));
  }

  @Override
  protected boolean deleteObject(String key) {
    try {
      mClient.deleteObject(new DeleteObjectInput().setBucket(mBucketName).setKey(key));
    } catch (TosClientException e) {
      LOG.error("Failed to delete {}", key, e);
      return false;
    } catch (TosServerException e) {
      LOG.error("Failed to delete {}", key, e);
      return false;
    }
    return true;
  }

  @Override
  protected List<String> deleteObjects(List<String> keys) throws IOException {
    try {
      List<ObjectTobeDeleted> objs = new ArrayList<>();
      for (String key : keys) {
        objs.add(new ObjectTobeDeleted().setKey(key));
      }
      mClient.deleteMultiObjects(new DeleteMultiObjectsV2Input()
          .setBucket(mBucketName).setObjects(objs));
    } catch (TosClientException e) {
      throw new IOException("failed to delete objects", e);
    } catch (TosServerException e) {
      throw new IOException("failed to delete objects", e);
    }
    return keys;
  }

  @Override
  protected String getFolderSuffix() {
    return FOLDER_SUFFIX;
  }

  @Override
  protected ObjectListingChunk getObjectListingChunk(String key, boolean recursive)
      throws IOException {
    String delimiter = recursive ? "" : PATH_SEPARATOR;
    key = PathUtils.normalizePath(key, PATH_SEPARATOR);
    // In case key is root (empty string) do not normalize prefix
    key = key.equals(PATH_SEPARATOR) ? "" : key;

    ListObjectsType2Input input = new ListObjectsType2Input().setBucket(mBucketName)
        .setMaxKeys(getListingChunkLength(mUfsConf)).setDelimiter(delimiter)
        .setPrefix(key);

    ListObjectsType2Output result = getObjectListingChunk(input);
    if (result != null) {
      return new TOSObjectListingChunk(input, result);
    }
    return null;
  }

  // Get next chunk of listing result
  private ListObjectsType2Output getObjectListingChunk(ListObjectsType2Input input) {
    ListObjectsType2Output output;
    try {
      output = mClient.listObjectsType2(input);
    } catch (TosClientException e) {
      LOG.error("Failed to list path {}", input.getPrefix(), e);
      output = null;
    } catch (TosServerException e) {
      LOG.error("Failed to list path {}", input.getPrefix(), e);
      output = null;
    }
    return output;
  }

  /**
   * Wrapper over TOS {@link ObjectListingChunk}.
   */
  private final class TOSObjectListingChunk implements ObjectListingChunk {
    final ListObjectsType2Input mInput;
    final ListObjectsType2Output mOutput;

    TOSObjectListingChunk(ListObjectsType2Input input, ListObjectsType2Output output) {
      Preconditions.checkNotNull(input, "input");
      Preconditions.checkNotNull(output, "output");
      mInput = input;
      mOutput = output;
    }

    @Override
    public ObjectStatus[] getObjectStatuses() {
      List<ListedObjectV2> objects = mOutput.getContents();
      ObjectStatus[] ret = new ObjectStatus[objects.size()];
      int i = 0;
      for (ListedObjectV2 obj : objects) {
        ret[i++] = new ObjectStatus(obj.getKey(), obj.getEtag(), obj.getSize(),
            obj.getLastModified().getTime());
      }
      return ret;
    }

    @Override
    public String[] getCommonPrefixes() {
      List<ListedCommonPrefix> res = mOutput.getCommonPrefixes();
      return res.toArray(new String[0]);
    }

    @Override
    public ObjectListingChunk getNextChunk() throws IOException {
      if (mOutput.isTruncated()) {
        mInput.setContinuationToken(mOutput.getNextContinuationToken());
        ListObjectsType2Output nextOutput = mClient.listObjectsType2(mInput);
        if (nextOutput != null) {
          return new TOSObjectListingChunk(mInput, nextOutput);
        }
      }
      return null;
    }
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    // Root is always a folder
    if (isRoot(path) || path.equals(PATH_SEPARATOR)) {
      return true;
    }
    String keyAsFolder = convertToFolderName(stripPrefixIfPresent(path));
    if (getObjectStatus(keyAsFolder) != null) {
      return true;
    }
    return getObjectListingChunkForPath(path, true) != null;
  }

  @Override
  public PositionReader openPositionRead(String path, long fileLength) {
    return new TOSPositionReader(mClient, mBucketName,
        stripPrefixIfPresent(path), fileLength);
  }

  @Override
  protected ObjectStatus getObjectStatus(String key) {
    try {
      HeadObjectV2Input input = new HeadObjectV2Input().setBucket(mBucketName).setKey(key);
      HeadObjectV2Output output = mClient.headObject(input);
      if (output == null) {
        return null;
      }
      return new ObjectStatus(key, output.getEtag(), output.getContentLength(),
          output.getLastModifiedInDate().getTime());
    } catch (TosClientException e) {
      LOG.error("Failed to get status for {}", key, e);
      return null;
    } catch (TosServerException e) {
      LOG.error("Failed to get status for {}", key, e);
      return null;
    }
  }

  // No ACL integration currently, returns default empty value
  @Override
  protected ObjectPermissions getPermissions() {
    return new ObjectPermissions("", "", Constants.DEFAULT_FILE_SYSTEM_MODE);
  }

  @Override
  protected String getRootKey() {
    return Constants.HEADER_TOS + mBucketName;
  }

  @Override
  protected InputStream openObject(String key, OpenOptions options,
      RetryPolicy retryPolicy) throws IOException {
    try {
      return new TOSInputStream(mBucketName, key, mClient, options.getOffset(), retryPolicy,
          mUfsConf.getBytes(PropertyKey.UNDERFS_OBJECT_STORE_MULTI_RANGE_CHUNK_SIZE));
    } catch (TosClientException e) {
      throw new IOException(e.getMessage());
    }
  }
}
