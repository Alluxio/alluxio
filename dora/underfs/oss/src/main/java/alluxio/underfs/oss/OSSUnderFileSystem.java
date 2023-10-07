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

package alluxio.underfs.oss;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.PositionReader;
import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.retry.RetryPolicy;
import alluxio.underfs.ObjectLowLevelOutputStream;
import alluxio.underfs.ObjectMultipartUploader;
import alluxio.underfs.ObjectUnderFileSystem;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.ListMultiPartOptions;
import alluxio.underfs.options.MultipartUfsOptions;
import alluxio.underfs.options.OpenOptions;
import alluxio.underfs.response.ListMultipartUploadResult;
import alluxio.underfs.response.MultipartUploadInfo;
import alluxio.underfs.response.PartSummaryInfo;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.io.PathUtils;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.ServiceException;
import com.aliyun.oss.common.comm.Protocol;
import com.aliyun.oss.common.utils.DateUtil;
import com.aliyun.oss.internal.Mimetypes;
import com.aliyun.oss.model.AbortMultipartUploadRequest;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.DeleteObjectsResult;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.ListMultipartUploadsRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.ListPartsRequest;
import com.aliyun.oss.model.MultipartUpload;
import com.aliyun.oss.model.MultipartUploadListing;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.PartListing;
import com.aliyun.oss.model.UploadPartRequest;
import com.amazonaws.AmazonClientException;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Aliyun OSS {@link UnderFileSystem} implementation.
 */
@ThreadSafe
public class OSSUnderFileSystem extends ObjectUnderFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(OSSUnderFileSystem.class);

  /** Suffix for an empty file to flag it as a directory. */
  private static final String FOLDER_SUFFIX = "_$folder$";

  /** Aliyun OSS client. */
  private final OSS mClient;

  /** Bucket name of user's configured Alluxio bucket. */
  private final String mBucketName;

  /** The executor service for the streaming upload. */
  private final Supplier<ListeningExecutorService> mStreamingUploadExecutor;

  /** The executor service for the multipart upload. */
  private final Supplier<ListeningExecutorService> mMultipartUploadExecutor;

  private StsOssClientProvider mClientProvider;

  /**
   * Constructs a new instance of {@link OSSUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @return the created {@link OSSUnderFileSystem} instance
   */
  public static OSSUnderFileSystem createInstance(AlluxioURI uri, UnderFileSystemConfiguration conf)
      throws Exception {
    String bucketName = UnderFileSystemUtils.getBucketName(uri);
    return new OSSUnderFileSystem(uri, null, bucketName, conf);
  }

  /**
   * Constructor for {@link OSSUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param ossClient Aliyun OSS client
   * @param bucketName bucket name of user's configured Alluxio bucket
   * @param conf configuration for this UFS
   */
  protected OSSUnderFileSystem(AlluxioURI uri, @Nullable OSS ossClient, String bucketName,
                               UnderFileSystemConfiguration conf) {
    super(uri, conf);

    if (conf.getBoolean(PropertyKey.UNDERFS_OSS_STS_ENABLED)) {
      try {
        mClientProvider = new StsOssClientProvider(conf);
        mClientProvider.init();
        mClient = mClientProvider.getOSSClient();
      } catch (IOException e) {
        LOG.error("init sts client provider failed!", e);
        throw new ServiceException(e);
      }
    } else if (null != ossClient) {
      mClient = ossClient;
    } else {
      Preconditions.checkArgument(conf.isSet(PropertyKey.OSS_ACCESS_KEY),
          "Property %s is required to connect to OSS", PropertyKey.OSS_ACCESS_KEY);
      Preconditions.checkArgument(conf.isSet(PropertyKey.OSS_SECRET_KEY),
          "Property %s is required to connect to OSS", PropertyKey.OSS_SECRET_KEY);
      Preconditions.checkArgument(conf.isSet(PropertyKey.OSS_ENDPOINT_KEY),
          "Property %s is required to connect to OSS", PropertyKey.OSS_ENDPOINT_KEY);
      String accessId = conf.getString(PropertyKey.OSS_ACCESS_KEY);
      String accessKey = conf.getString(PropertyKey.OSS_SECRET_KEY);
      String endPoint = conf.getString(PropertyKey.OSS_ENDPOINT_KEY);

      ClientBuilderConfiguration ossClientConf = initializeOSSClientConfig(conf);
      mClient = new OSSClientBuilder().build(endPoint, accessId, accessKey, ossClientConf);
    }

    mBucketName = bucketName;

    // Initialize the executor service for the streaming upload.
    mStreamingUploadExecutor = Suppliers.memoize(() -> {
      int numTransferThreads =
          conf.getInt(PropertyKey.UNDERFS_OSS_STREAMING_UPLOAD_THREADS);
      ExecutorService service = ExecutorServiceFactories
          .fixedThreadPool("alluxio-oss-streaming-upload-worker",
              numTransferThreads).create();
      return MoreExecutors.listeningDecorator(service);
    });

    // Initialize the executor service for the multipart upload.
    mMultipartUploadExecutor = Suppliers.memoize(() -> {
      int numTransferThreads =
          conf.getInt(PropertyKey.UNDERFS_OSS_MULTIPART_UPLOAD_THREADS);
      ExecutorService service = ExecutorServiceFactories
          .fixedThreadPool("alluxio-oss-multipart-upload-worker",
              numTransferThreads).create();
      return MoreExecutors.listeningDecorator(service);
    });
  }

  @Override
  public void cleanup() throws IOException {
    long cleanAge = mUfsConf.getMs(PropertyKey.UNDERFS_OSS_INTERMEDIATE_UPLOAD_CLEAN_AGE);
    Date cleanBefore = new Date(new Date().getTime() - cleanAge);
    MultipartUploadListing uploadListing = mClient.listMultipartUploads(
        new ListMultipartUploadsRequest(mBucketName));
    do {
      for (MultipartUpload upload : uploadListing.getMultipartUploads()) {
        if (upload.getInitiated().compareTo(cleanBefore) < 0) {
          mClient.abortMultipartUpload(new AbortMultipartUploadRequest(
              mBucketName, upload.getKey(), upload.getUploadId()));
        }
      }
      ListMultipartUploadsRequest request = new ListMultipartUploadsRequest(mBucketName);
      request.setUploadIdMarker(uploadListing.getNextUploadIdMarker());
      request.setKeyMarker(uploadListing.getKeyMarker());
      uploadListing = mClient.listMultipartUploads(request);
    } while (uploadListing.isTruncated());
  }

  @Override
  public String getUnderFSType() {
    return "oss";
  }

  @Override
  public PositionReader openPositionRead(String path, long fileLength) {
    return new OSSPositionReader(mClient, mBucketName, stripPrefixIfPresent(path), fileLength);
  }

  // No ACL integration currently, no-op
  @Override
  public void setOwner(String path, String user, String group) {}

  // No ACL integration currently, no-op
  @Override
  public void setMode(String path, short mode) throws IOException {}

  @Override
  protected boolean copyObject(String src, String dst) {
    LOG.debug("Copying {} to {}", src, dst);
    try {
      mClient.copyObject(mBucketName, src, mBucketName, dst);
      return true;
    } catch (ServiceException e) {
      LOG.error("Failed to rename file {} to {}", src, dst, e);
      return false;
    }
  }

  @Override
  public boolean createEmptyObject(String key) {
    try {
      ObjectMetadata objMeta = new ObjectMetadata();
      objMeta.setContentLength(0);
      mClient.putObject(mBucketName, key, new ByteArrayInputStream(new byte[0]), objMeta);
      return true;
    } catch (ServiceException e) {
      LOG.error("Failed to create object: {}", key, e);
      return false;
    }
  }

  @Override
  protected OutputStream createObject(String key) throws IOException {
    if (mUfsConf.getBoolean(PropertyKey.UNDERFS_OSS_STREAMING_UPLOAD_ENABLED)) {
      ObjectMultipartUploader multipartUploader =
          new ObjectMultipartUploader(key, this, mStreamingUploadExecutor.get());
      return new ObjectLowLevelOutputStream(mBucketName, key, multipartUploader, mUfsConf);
    }
    else if (mUfsConf.getBoolean(PropertyKey.UNDERFS_OSS_MULTIPART_UPLOAD_ENABLED)) {
      return new OSSMultipartUploadOutputStream(mBucketName, key, mClient,
          mMultipartUploadExecutor.get(), mUfsConf);
    }
    return new OSSOutputStream(mBucketName, key, mClient,
        mUfsConf.getList(PropertyKey.TMP_DIRS));
  }

  @Override
  protected boolean deleteObject(String key) {
    try {
      mClient.deleteObject(mBucketName, key);
    } catch (ServiceException e) {
      LOG.error("Failed to delete {}", key, e);
      return false;
    }
    return true;
  }

  @Override
  protected List<String> deleteObjects(List<String> keys) throws IOException {
    try {
      DeleteObjectsRequest request = new DeleteObjectsRequest(mBucketName);
      request.setKeys(keys);
      DeleteObjectsResult result = mClient.deleteObjects(request);
      return result.getDeletedObjects();
    } catch (ServiceException e) {
      throw new IOException("Failed to delete objects", e);
    }
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
    ListObjectsRequest request = new ListObjectsRequest(mBucketName);
    request.setPrefix(key);
    request.setMaxKeys(getListingChunkLength(mUfsConf));
    request.setDelimiter(delimiter);

    ObjectListing result = getObjectListingChunk(request);
    if (result != null) {
      return new OSSObjectListingChunk(request, result);
    }
    return null;
  }

  // Get next chunk of listing result
  protected ObjectListing getObjectListingChunk(ListObjectsRequest request) {
    ObjectListing result;
    try {
      result = mClient.listObjects(request);
    } catch (ServiceException e) {
      LOG.error("Failed to list path {}", request.getPrefix(), e);
      result = null;
    }
    return result;
  }

  /**
   * Wrapper over OSS {@link ObjectListingChunk}.
   */
  private final class OSSObjectListingChunk implements ObjectListingChunk {
    final ListObjectsRequest mRequest;
    final ObjectListing mResult;

    OSSObjectListingChunk(ListObjectsRequest request, ObjectListing result) throws IOException {
      mRequest = request;
      mResult = result;
      if (mResult == null) {
        throw new IOException("OSS listing result is null");
      }
    }

    @Override
    public ObjectStatus[] getObjectStatuses() {
      List<OSSObjectSummary> objects = mResult.getObjectSummaries();
      ObjectStatus[] ret = new ObjectStatus[objects.size()];
      int i = 0;
      for (OSSObjectSummary obj : objects) {
        Date lastModifiedDate = obj.getLastModified();
        Long lastModifiedTime = lastModifiedDate == null ? null : lastModifiedDate.getTime();
        ret[i++] = new ObjectStatus(obj.getKey(), obj.getETag(), obj.getSize(),
            lastModifiedTime);
      }
      return ret;
    }

    @Override
    public String[] getCommonPrefixes() {
      List<String> res = mResult.getCommonPrefixes();
      return res.toArray(new String[0]);
    }

    @Override
    public ObjectListingChunk getNextChunk() throws IOException {
      if (mResult.isTruncated()) {
        mRequest.setMarker(mResult.getNextMarker());
        ObjectListing nextResult = mClient.listObjects(mRequest);
        if (nextResult != null) {
          return new OSSObjectListingChunk(mRequest, nextResult);
        }
      }
      return null;
    }
  }

  @Override
  protected ObjectStatus getObjectStatus(String key) {
    try {
      ObjectMetadata meta = mClient.getObjectMetadata(mBucketName, key);
      if (meta == null) {
        return null;
      }
      Date lastModifiedDate = meta.getLastModified();
      Long lastModifiedTime = lastModifiedDate == null ? null : lastModifiedDate.getTime();
      return new ObjectStatus(key, meta.getETag(), meta.getContentLength(),
          lastModifiedTime);
    } catch (ServiceException e) {
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
    return Constants.HEADER_OSS + mBucketName;
  }

  /**
   * Creates an OSS {@code ClientConfiguration} using an Alluxio Configuration.
   * @param alluxioConf the OSS Configuration
   * @return the OSS {@link ClientBuilderConfiguration}
   */
  public static ClientBuilderConfiguration initializeOSSClientConfig(
      AlluxioConfiguration alluxioConf) {
    ClientBuilderConfiguration ossClientConf = new ClientBuilderConfiguration();
    ossClientConf
        .setConnectionTimeout((int) alluxioConf.getMs(PropertyKey.UNDERFS_OSS_CONNECT_TIMEOUT));
    ossClientConf.setSocketTimeout((int) alluxioConf.getMs(PropertyKey.UNDERFS_OSS_SOCKET_TIMEOUT));
    ossClientConf.setConnectionTTL(alluxioConf.getMs(PropertyKey.UNDERFS_OSS_CONNECT_TTL));
    ossClientConf.setMaxConnections(alluxioConf.getInt(PropertyKey.UNDERFS_OSS_CONNECT_MAX));
    ossClientConf.setMaxErrorRetry(alluxioConf.getInt(PropertyKey.UNDERFS_OSS_RETRY_MAX));
    if (isProxyEnabled(alluxioConf)) {
      String proxyHost = getProxyHost(alluxioConf);
      int proxyPort = getProxyPort(alluxioConf);
      ossClientConf.setProxyHost(proxyHost);
      ossClientConf.setProxyPort(proxyPort);
      ossClientConf.setProtocol(getProtocol());
      LOG.info("the proxy for OSS is enabled, the proxy endpoint is: {}:{}", proxyHost, proxyPort);
    }
    return ossClientConf;
  }

  private static boolean isProxyEnabled(AlluxioConfiguration alluxioConf) {
    return getProxyHost(alluxioConf) != null && getProxyPort(alluxioConf) > 0;
  }

  private static int getProxyPort(AlluxioConfiguration alluxioConf) {
    int proxyPort = alluxioConf.getInt(PropertyKey.UNDERFS_OSS_PROXY_PORT);
    if (proxyPort >= 0) {
      return proxyPort;
    } else {
      try {
        return getProxyPortFromSystemProperty();
      } catch (NumberFormatException e) {
        return proxyPort;
      }
    }
  }

  private static String getProxyHost(AlluxioConfiguration alluxioConf) {
    String proxyHost = alluxioConf.getOrDefault(PropertyKey.UNDERFS_OSS_PROXY_HOST, null);
    if (proxyHost != null) {
      return proxyHost;
    } else {
      return getProxyHostFromSystemProperty();
    }
  }

  private static Protocol getProtocol() {
    String protocol = Configuration.getString(PropertyKey.UNDERFS_OSS_PROTOCOL);
    return protocol.equals(Protocol.HTTPS.toString()) ? Protocol.HTTPS : Protocol.HTTP;
  }

  /**
   * Returns the Java system property for proxy port depending on
   * {@link #getProtocol()}: i.e. if protocol is https, returns
   * the value of the system property https.proxyPort, otherwise
   * returns value of http.proxyPort.  Defaults to {@link this.proxyPort}
   * if the system property is not set with a valid port number.
   */
  private static int getProxyPortFromSystemProperty() {
    return getProtocol() == Protocol.HTTPS
        ? Integer.parseInt(getSystemProperty("https.proxyPort"))
        : Integer.parseInt(getSystemProperty("http.proxyPort"));
  }

  /**
   * Returns the Java system property for proxy host depending on
   * {@link #getProtocol()}: i.e. if protocol is https, returns
   * the value of the system property https.proxyHost, otherwise
   * returns value of http.proxyHost.
   */
  private static String getProxyHostFromSystemProperty() {
    return getProtocol() == Protocol.HTTPS
        ? getSystemProperty("https.proxyHost")
        : getSystemProperty("http.proxyHost");
  }

  /**
   * Returns the value for the given system property.
   */
  private static String getSystemProperty(String property) {
    return System.getProperty(property);
  }

  @Override
  protected InputStream openObject(String key, OpenOptions options, RetryPolicy retryPolicy)
      throws IOException {
    try {
      return new OSSInputStream(mBucketName, key, mClient, options.getOffset(), retryPolicy,
          mUfsConf.getBytes(PropertyKey.UNDERFS_OBJECT_STORE_MULTI_RANGE_CHUNK_SIZE));
    } catch (ServiceException e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  protected String initMultiPart(String key, MultipartUfsOptions options) throws IOException {
    try {
      ObjectMetadata meta = new ObjectMetadata();
      meta.setContentType(Mimetypes.DEFAULT_MIMETYPE);
      InitiateMultipartUploadRequest initRequest =
          new InitiateMultipartUploadRequest(mBucketName, key, meta);
      return mClient.initiateMultipartUpload(initRequest).getUploadId();
    } catch (OSSException | ClientException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected String uploadPartWithStream(String key, String uploadId, int partNum, long fileSize,
                                        InputStream stream, MultipartUfsOptions options)
      throws IOException {
    try {
      final UploadPartRequest uploadRequest =
          new UploadPartRequest(mBucketName, key, uploadId, partNum, stream, fileSize);
      PartETag partETag = mClient.uploadPart(uploadRequest).getPartETag();
      return partETag.getETag();
    } catch (OSSException | ClientException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected String completeMultiPart(String key, String uploadId, List<Pair<Integer, String>> etags,
                                     MultipartUfsOptions options) throws IOException {
    try {
      List<PartETag> partETags = new ArrayList<>();
      for (Pair pair : etags) {
        partETags.add(new PartETag((Integer) pair.getFirst(), (String) pair.getSecond()));
      }
      CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(
          mBucketName, key, uploadId, partETags);
      return mClient.completeMultipartUpload(completeRequest).getETag();
    } catch (OSSException | ClientException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected List<PartSummaryInfo> listParts(String key, String uploadId,
                                            MultipartUfsOptions options) throws IOException {
    try {
      ListPartsRequest request = new ListPartsRequest(mBucketName, key, uploadId);
      PartListing result = mClient.listParts(request);
      List<PartSummaryInfo> partList = result.getParts().stream().map(
          part -> new PartSummaryInfo(part.getPartNumber(),
              DateUtil.formatIso8601Date(part.getLastModified()), part.getETag(),
              part.getSize())).collect(Collectors.toList());
      return partList;
    } catch (OSSException | ClientException e) {
      throw new IOException(e);
    }
  }

  @Override
  public ListMultipartUploadResult listMultipartUploads(ListMultiPartOptions options)
      throws IOException {
    ListMultipartUploadsRequest request = new ListMultipartUploadsRequest(mBucketName);
    if (options.getPrefix() != null) {
      request.setPrefix(stripPrefixIfPresent(options.getPrefix()));
    }
    if (options.getDelimiter() != null) {
      request.setDelimiter(options.getDelimiter());
    }
    if (options.getKeyMarker() != null) {
      request.setKeyMarker(stripPrefixIfPresent(options.getKeyMarker()));
    }
    if (options.getUploadIdMarker() != null) {
      request.setUploadIdMarker(options.getUploadIdMarker());
    }
    if (options.getMaxUploads() >= 0) {
      request.setMaxUploads(options.getMaxUploads());
    }
    try {
      MultipartUploadListing result = mClient.listMultipartUploads(request);
      List<MultipartUploadInfo> uploadInfoList = result.getMultipartUploads().stream().map(
              task -> new MultipartUploadInfo(Constants.HEADER_S3 + mBucketName + "/" + task.getKey(),
                  task.getUploadId(), DateUtil.formatIso8601Date(task.getInitiated())))
          .collect(Collectors.toList());
      ListMultipartUploadResult listResult = new ListMultipartUploadResult(uploadInfoList);
      listResult.setPrefix(result.getPrefix());
      listResult.setDelimiter(result.getDelimiter());
      listResult.setMaxUploads(result.getMaxUploads());
      listResult.setTruncated(result.isTruncated());
      listResult.setKeyMarker(Constants.HEADER_S3 + mBucketName + "/" + result.getKeyMarker());
      listResult.setUploadIdMarker(result.getUploadIdMarker());
      listResult.setNextKeyMarker(
          Constants.HEADER_S3 + mBucketName + "/" + result.getNextKeyMarker());
      listResult.setNextUploadIdMarker(result.getNextUploadIdMarker());
      return listResult;
    } catch (OSSException | ClientException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void abortMultipartTask(String key, String uploadId, MultipartUfsOptions options)
      throws IOException {
    try {
      mClient.abortMultipartUpload(new AbortMultipartUploadRequest(mBucketName, key, uploadId));
    } catch (OSSException | ClientException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    mClientProvider.close();
  }
}
