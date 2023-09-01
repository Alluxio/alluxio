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

package alluxio.worker.s3;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.DoraCacheFileSystem;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.audit.AsyncUserAccessAuditLogWriter;
import alluxio.network.netty.FileTransferType;
import alluxio.network.protocol.databuffer.CompositeDataBuffer;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataFileChannel;
import alluxio.network.protocol.databuffer.NettyDataBuffer;
import alluxio.network.protocol.databuffer.NioDataBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.proto.journal.File;
import alluxio.s3.NettyRestUtils;
import alluxio.s3.S3AuditContext;
import alluxio.s3.S3Constants;
import alluxio.s3.S3ErrorCode;
import alluxio.s3.S3Exception;
import alluxio.security.User;
import alluxio.util.CommonUtils;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.dora.DoraWorker;

import com.google.common.base.Stopwatch;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.security.auth.Subject;

/**
 * S3 Netty Handler - handle http requests from S3 API in Netty.
 */
public class S3NettyHandler {
  private static final Logger LOG = LoggerFactory.getLogger(S3NettyHandler.class);
  private String mUser;
  private final String mBucket;
  private final String mObject;
  private final HttpRequest mRequest;
  private BlockingQueue<HttpContent> mContentQueue;
  private HttpResponse mResponse;
  private final ChannelHandlerContext mContext;
  private final QueryStringDecoder mQueryDecoder;
  private S3NettyBaseTask mS3Task;
  private FileSystem mFsClient;
  private FileSystem mUserFsClient;
  private DoraWorker mDoraWorker;
  private Stopwatch mStopwatch;
  public AsyncUserAccessAuditLogWriter mAsyncAuditLogWriter;
  private final FileTransferType mFileTransferType;

  public static final Pattern BUCKET_PATH_PATTERN = Pattern.compile("^" + "/[^/]*$");
  public static final Pattern OBJECT_PATH_PATTERN = Pattern.compile("^" + "/[^/]*/.*$");
  public static final boolean BUCKET_NAMING_RESTRICTION_ENABLED =
      Configuration.getBoolean(PropertyKey.PROXY_S3_BUCKET_NAMING_RESTRICTIONS_ENABLED);
  public static final WritePType S3_WRITE_TYPE =
      Configuration.getEnum(PropertyKey.PROXY_S3_WRITE_TYPE, WriteType.class).toProto();
  // https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
  // - Undocumented edge-case, no adjacent periods with hyphens, i.e: '.-' or '-.'
  public static final Pattern BUCKET_ADJACENT_DOTS_DASHES_PATTERN = Pattern.compile("([-\\.]{2})");
  public static final Pattern BUCKET_INVALIDATION_PREFIX_PATTERN = Pattern.compile("^xn--.*");
  public static final Pattern BUCKET_INVALID_SUFFIX_PATTERN = Pattern.compile(".*-s3alias$");
  public static final Pattern BUCKET_VALID_NAME_PATTERN =
      Pattern.compile("[a-z0-9][a-z0-9\\.-]{1,61}[a-z0-9]");
  public static final int BUCKET_PATH_CACHE_SIZE = 65536;
  /* BUCKET_PATH_CACHE caches bucket path during specific period.
   BUCKET_PATH_CACHE.put(bucketPath,true) means bucket path exists.
   BUCKET_PATH_CACHE.put(bucketPath,false) plays the same effect
   as BUCKET_PATH_CACHE.remove(bucketPath). */
  public static final Cache<String, Boolean> BUCKET_PATH_CACHE = CacheBuilder.newBuilder()
      .maximumSize(BUCKET_PATH_CACHE_SIZE)
      .expireAfterWrite(
          Configuration.global().getMs(PropertyKey.PROXY_S3_BUCKETPATHCACHE_TIMEOUT_MS),
          TimeUnit.MILLISECONDS)
      .build();
  private static final int PACKET_LENGTH = 8 * 1024;
  private static final String[] UNSUPPORTED_SUB_RESOURCES = {"acl", "policy", "versioning", "cors",
      "encryption", "intelligent-tiering", "inventory", "lifecycle",
      "metrics", "ownershipControls", "replication", "website", "accelerate",
      "location", "logging", "metrics", "notification", "ownershipControls",
      "policyStatus", "requestPayment", "attributes", "legal-hold", "object-lock",
      "retention", "torrent", "publicAccessBlock", "restore", "select",
      "tagging", "uploads", "uploadId"};
  private static final Set<String> UNSUPPORTED_SUB_RESOURCES_SET =
      new HashSet<>(Arrays.asList(UNSUPPORTED_SUB_RESOURCES));
  Map<String, String> mAmzHeaderMap = new HashMap<>();

  /**
   * Constructs an instance of {@link S3NettyHandler}.
   * @param bucket
   * @param object
   * @param request
   * @param ctx
   * @param fileSystem
   * @param doraWorker
   * @param asyncAuditLogWriter
   */
  public S3NettyHandler(String bucket, String object, HttpRequest request,
                        ChannelHandlerContext ctx, FileSystem fileSystem,
                        DoraWorker doraWorker,
                        AsyncUserAccessAuditLogWriter asyncAuditLogWriter) {
    mBucket = bucket;
    mObject = object;
    mRequest = request;
    mContext = ctx;
    mFsClient = fileSystem;
    mDoraWorker = doraWorker;
    mQueryDecoder = new QueryStringDecoder(request.uri());
    mFileTransferType = Configuration
        .getEnum(PropertyKey.WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE, FileTransferType.class);
    mAsyncAuditLogWriter = asyncAuditLogWriter;
  }

  /**
   * Create a S3Handler based on the incoming Request.
   * @param context
   * @param request
   * @param fileSystem
   * @param doraWorker
   * @param asyncAuditLogWriter
   * @return A S3Handler
   * @throws Exception
   *
   */
  public static S3NettyHandler createHandler(ChannelHandlerContext context, HttpRequest request,
                                             FileSystem fileSystem, DoraWorker doraWorker,
                                             AsyncUserAccessAuditLogWriter asyncAuditLogWriter)
      throws Exception {
    String path = java.net.URI.create(request.uri()).getPath();
    Stopwatch stopwatch = Stopwatch.createStarted();
    Matcher bucketMatcher = BUCKET_PATH_PATTERN.matcher(path);
    Matcher objectMatcher = OBJECT_PATH_PATTERN.matcher(path);
    String pathStr = path.substring(1);
    String bucket = null;
    String object = null;
    if (bucketMatcher.matches()) {
      bucket = URLDecoder.decode(pathStr, "UTF-8");
    } else if (objectMatcher.matches()) {
      bucket = URLDecoder.decode(
          pathStr.substring(0, pathStr.indexOf(AlluxioURI.SEPARATOR)), "UTF-8");
      object = URLDecoder.decode(
          pathStr.substring(pathStr.indexOf(AlluxioURI.SEPARATOR) + 1), "UTF-8");
    }
    S3NettyHandler handler =
        new S3NettyHandler(bucket, object, request, context, fileSystem, doraWorker,
            asyncAuditLogWriter);
    handler.setStopwatch(stopwatch);
    handler.init();
    S3NettyBaseTask task = null;
    if (object != null && !object.isEmpty()) {
      task = S3NettyObjectTask.Factory.create(handler);
    } else {
      task = S3NettyBucketTask.Factory.create(handler);
    }
    handler.setS3Task(task);
    return handler;
  }

  /**
   * Initialize the S3Handler object in preparation for handling the request.
   * @throws Exception
   */
  public void init() throws Exception {
    // Do Authentication of the request.
    doAuthentication();
    // Extract x-amz- headers.
    extractAMZHeaders();
    // Reject unsupported subresources.
    rejectUnsupportedResources();
    // Init utils
    mContentQueue = new LinkedBlockingQueue<>();

    // TODO(wyy) init directories
    // Initiate the S3 API MPU metadata directories
  }

  /**
   * Utility function to help extract x-amz- headers from request.
   */
  public void extractAMZHeaders() {
    Iterator<Map.Entry<String, String>> headerIt = mRequest.headers().iteratorAsString();
    while (headerIt.hasNext()) {
      Map.Entry<String, String> header = headerIt.next();
      mAmzHeaderMap.putIfAbsent(header.getKey(), header.getValue());
    }
  }

  /**
   * Reject unsupported request from the given subresources from request.
   * @throws S3Exception
   */
  public void rejectUnsupportedResources() throws S3Exception {
    String uri = mRequest.uri();
    QueryStringDecoder decoder = new QueryStringDecoder(uri);
    Map<String, List<String>> parameters = decoder.parameters();
    for (String parameter : parameters.keySet()) {
      if (UNSUPPORTED_SUB_RESOURCES_SET.contains(parameter)) {
        throw new S3Exception(S3Constants.EMPTY, S3ErrorCode.NOT_IMPLEMENTED);
      }
    }
  }

  /**
   * Do S3 request authentication.
   * @throws Exception
   */
  public void doAuthentication() throws Exception {
    try {
      mUser = NettyRestUtils.getUser(mRequest);
    } catch (Exception e) {
      LOG.warn("exception happened in Authentication.");
      throw e;
    }
  }

  /**
   * Creates a {@link S3AuditContext} instance.
   *
   * @param command the command to be logged by this {@link S3AuditContext}
   * @param user  user name
   * @param bucket  bucket name
   * @param object  object name
   * @return newly-created {@link S3AuditContext} instance
   */
  public S3AuditContext createAuditContext(String command,
                                           String user,
                                           @Nullable String bucket,
                                           @Nullable String object) {
    // Audit log may be enabled during runtime
    AsyncUserAccessAuditLogWriter auditLogWriter = null;
    if (Configuration.getBoolean(PropertyKey.MASTER_AUDIT_LOGGING_ENABLED)) {
      auditLogWriter = mAsyncAuditLogWriter;
    }
    S3AuditContext auditContext = new S3AuditContext(auditLogWriter);
    if (auditLogWriter != null) {
      String ugi = "";
      if (user != null) {
        try {
          String primaryGroup = CommonUtils.getPrimaryGroupName(user, Configuration.global());
          ugi = user + "," + primaryGroup;
        } catch (IOException e) {
          LOG.debug("Failed to get primary group for user {}.", user);
          ugi = user + ",N/A";
        }
      } else {
        ugi = "N/A";
      }
      InetSocketAddress remoteAddress = (InetSocketAddress) mContext.channel().remoteAddress();
      auditContext.setUgi(ugi)
          .setCommand(command)
          .setIp(String.format("%s:%s",
              remoteAddress.getAddress(), remoteAddress.getPort()))
          .setBucket(bucket)
          .setObject(object)
          .setAllowed(true)
          .setSucceeded(true)
          .setCreationTimeNs(System.nanoTime());
    }
    return auditContext;
  }

  /**
   * Writes HttpResponse into context channel, After writes context channel will close.
   * @param response HttpResponse object
   */
  public void processHttpResponse(HttpResponse response) {
    processHttpResponse(response, true);
  }

  /**
   * Writes HttpResponse into context channel.
   * @param response HttpResponse object
   * @param closeAfterWrite if true, After writes context channel will close
   */
  public void processHttpResponse(HttpResponse response, boolean closeAfterWrite) {
    if (response != null) {
      setResponse(response);
      ChannelFuture future = mContext.writeAndFlush(response);
      if (closeAfterWrite) {
        future.addListener(new S3NettyFutureListener(this));
      }
      return;
    }
    if (closeAfterWrite) {
      mContext.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT)
          .addListener(new S3NettyFutureListener(this));
    }
  }

  /**
   * Writes a {@link DataBuffer} into netty channel. It supports zero copy through ByteBuf and
   * FileRegion.
   * @param packet DataBuffer packet
   */
  public void processTransferResponse(DataBuffer packet) {
    // Send data to client
    if (packet instanceof NettyDataBuffer || packet instanceof NioDataBuffer) {
      ByteBuf buf = (ByteBuf) packet.getNettyOutput();
      mContext.write(buf);
    } else if (packet instanceof DataFileChannel) {
      FileRegion fileRegion = (FileRegion) packet.getNettyOutput();
      mContext.write(fileRegion);
    } else if (packet instanceof CompositeDataBuffer) {
      // add each channel to output
      List<DataBuffer> dataFileChannels = (List<DataBuffer>) packet.getNettyOutput();
      for (DataBuffer dataFileChannel : dataFileChannels) {
        mContext.write(dataFileChannel.getNettyOutput());
      }
    } else {
      throw new IllegalArgumentException("Unexpected payload type");
    }
  }

  /**
   * Writes data into netty channel by copying through ByteBuf.
   * @param blockReader reader instance
   * @param objectSize size of the object
   * @throws IOException
   */
  public void processMappedResponse(BlockReader blockReader, long objectSize) throws IOException {
    int packetSize = PACKET_LENGTH;
    if (objectSize < (long) PACKET_LENGTH) {
      packetSize = (int) objectSize;
    }
    ByteBuf buf = mContext.channel().alloc().buffer(packetSize, packetSize);
    try {
      while (buf.writableBytes() > 0 && blockReader.transferTo(buf) != -1) {
        mContext.write(new DefaultHttpContent(buf));
        buf = mContext.channel().alloc().buffer(packetSize, packetSize);
      }
    } catch (Exception e) {
      throw e;
    }
  }

  /**
   * Gets a {@link BlockReader} according the ufs full path, offset and length.
   * @param ufsFullPath UFS full path
   * @param offset the offset of this reading
   * @param length the length of this reading
   * @return a BlockReader
   * @throws IOException
   * @throws AccessControlException
   */
  public BlockReader openBlock(String ufsFullPath, long offset, long length)
      throws IOException, AccessControlException {
    Protocol.OpenUfsBlockOptions options =
        Protocol.OpenUfsBlockOptions.newBuilder().setUfsPath(ufsFullPath).setMountId(0)
            .setNoCache(false).setOffsetInFile(offset).setBlockSize(length).build();
    BlockReader blockReader =
        mDoraWorker.createFileReader(new AlluxioURI(ufsFullPath).hash(), offset, false, options);
    if (blockReader.getChannel() instanceof FileChannel) {
      ((FileChannel) blockReader.getChannel()).position(offset);
    }
    return blockReader;
  }

  /**
   * set S3Task for this S3Handler.
   * @param task
   */
  public void setS3Task(S3NettyBaseTask task) {
    mS3Task = task;
  }

  /**
   * set FullHttpResponse for this request.
   * @param response
   */
  public void setResponse(HttpResponse response) {
    mResponse = response;
  }

  /**
   * Get the user name of this request.
   * @return user name
   */
  public String getUser() {
    return mUser;
  }

  /**
   * Get the bucket name of this request.
   * @return bucket name
   */
  public String getBucket() {
    return mBucket;
  }

  /**
   * Get the object name of this request.
   * @return object name
   */
  public String getObject() {
    return mObject;
  }

  /**
   * Get the channel context of this request.
   * @return ChannelHandlerContext
   */
  public ChannelHandlerContext getContext() {
    return mContext;
  }

  /**
   * Get the FileTransferType of the netty server.
   * @return ChannelHandlerContext
   */
  public FileTransferType getFileTransferType() {
    return mFileTransferType;
  }

  /**
   * get S3Task of this S3Handler.
   * @return S3BaseTask
   */
  public S3NettyBaseTask getS3Task() {
    return mS3Task;
  }

  /**
   * Get system user FileSystem object.
   * @return FileSystem object
   */
  public FileSystem getFsClient() {
    return mFsClient;
  }

  /**
   * Get dora worker object.
   * @return DoraWorker object
   */
  public DoraWorker getDoraWorker() {
    return mDoraWorker;
  }

  /**
   * get HTTP request.
   * @return HTTP request
   */
  public HttpRequest getRequest() {
    return mRequest;
  }

  /**
   * get HTTP response.
   * @return HTTP response
   */
  public HttpResponse getResponse() {
    return mResponse;
  }

  /**
   * get HTTP content of this request.
   * @return HTTP content
   */
  public HttpContent getLatestContent() throws InterruptedException {
    if (mContentQueue != null) {
      return mContentQueue.take();
    }
    return null;
  }

  /**
   * @return Does there exists content in the content queue
   */
  public boolean remainContent() {
    return !mContentQueue.isEmpty();
  }

  /**
   * Adds content to the content queue.
   * @param content
   * @return the result of adding content
   */
  public boolean addContent(HttpContent content) {
    if (mContentQueue != null) {
      return mContentQueue.offer(content);
    }
    return false;
  }

  /**
   * get HTTP verb of this request.
   * @return HTTP Verb
   */
  public String getHttpMethod() {
    return mRequest.method().name();
  }

  /**
   * get specified HTTP header value of this request.
   * @param headerName
   * @return header value
   */
  public String getHeader(String headerName) {
    return mRequest.headers().get(headerName);
  }

  /**
   * get specified HTTP header with a default if not exist.
   * @param headerName
   * @param defaultHeaderValue
   * @return header value
   */
  public String getHeaderOrDefault(String headerName, String defaultHeaderValue) {
    String headerVal = mRequest.headers().get(headerName);
    if (headerVal == null) {
      headerVal = defaultHeaderValue;
    }
    return headerVal;
  }

  /**
   * retrieve given query parameter value.
   * @param queryParam
   * @return query parameter value
   */
  @Nullable
  public String getQueryParameter(String queryParam) {
    if (mQueryDecoder.parameters().get(queryParam) != null) {
      return mQueryDecoder.parameters().get(queryParam).get(0);
    } else {
      return null;
    }
  }

  /**
   * Get Stopwatch object used for recording this request's latency.
   * @return Stopwatch object
   */
  public Stopwatch getStopwatch() {
    return mStopwatch;
  }

  /**
   * Set the Stopwatch object used for recording this request's latency.
   * @param stopwatch
   */
  public void setStopwatch(Stopwatch stopwatch) {
    mStopwatch = stopwatch;
  }

  /**
   * @param user the {@link Subject} name of the filesystem user
   * @return A {@link FileSystem} with the subject set to the provided user
   */
  public FileSystem getFileSystemForUser(String user) {
    if (user == null) {
      // Used to return the top-level FileSystem view when not using Authentication
      return mFsClient;
    }

    if (mUserFsClient == null) {
      final Subject subject = new Subject();
      subject.getPrincipals().add(new User(user));
      // Use local conf to create filesystem rather than fs.getConf()
      // due to fs conf will be changed by merged cluster conf.
      mUserFsClient = FileSystem.Factory.get(subject, Configuration.global());
    }
    return mUserFsClient;
  }

  /**
   * Gets UFS full path from Alluxio path.
   * @param objectPath the Alluxio path
   * @return UfsBaseFileSystem based full path
   */
  public AlluxioURI getUfsPath(AlluxioURI objectPath) throws S3Exception {
    if (mFsClient instanceof DoraCacheFileSystem) {
      return ((DoraCacheFileSystem) mFsClient).convertAlluxioPathToUFSPath(objectPath);
    } else {
      throw new S3Exception(objectPath.toString(), S3ErrorCode.INTERNAL_ERROR);
    }
  }

  /**
   * Check if a path in alluxio is a directory.
   *
   * @param fs instance of {@link FileSystem}
   * @param bucketPath bucket complete path
   * @param auditContext the audit context for exception
   */
  public static void checkPathIsAlluxioDirectory(FileSystem fs, String bucketPath,
                                          @Nullable S3AuditContext auditContext)
      throws S3Exception {
    if (Boolean.TRUE.equals(BUCKET_PATH_CACHE.getIfPresent(bucketPath))) {
      return;
    }
    try {
      // TODO(wyy) check bucket is alluxio directory in cache first
      URIStatus status = fs.getStatus(new AlluxioURI(bucketPath));
      if (!status.isFolder()) {
        throw new FileDoesNotExistException(
            ExceptionMessage.BUCKET_DOES_NOT_EXIST.getMessage(bucketPath));
      }
    } catch (Exception e) {
      throw NettyRestUtils.toBucketS3Exception(e, bucketPath, auditContext);
    }
    BUCKET_PATH_CACHE.put(bucketPath, true);
  }

  /**
   * This helper method is used to set the ETag xAttr on an object.
   * @param fs The {@link FileSystem} used to make the gRPC request
   * @param objectUri The {@link AlluxioURI} for the object to update
   * @param entityTag The entity tag of the object (MD5 checksum of the object contents)
   * @throws IOException
   * @throws AlluxioException
   */
  public static void setEntityTag(FileSystem fs, AlluxioURI objectUri, String entityTag)
      throws IOException, AlluxioException {
    fs.setAttribute(objectUri, SetAttributePOptions.newBuilder()
        .putXattr(S3Constants.ETAG_XATTR_KEY,
            ByteString.copyFrom(entityTag, S3Constants.XATTR_STR_CHARSET))
        .setXattrUpdateStrategy(File.XAttrUpdateStrategy.UNION_REPLACE)
        .build());
  }

  /**
   * Log the access of every single http request.
   * @param request
   * @param response
   * @param stopWatch
   * @param opType
   */
  public static void logAccess(HttpRequest request, HttpResponse response,
                               Stopwatch stopWatch, S3NettyBaseTask.OpType opType) {
    String contentLenStr = "None";
    if (request.headers().get("x-amz-decoded-content-length") != null) {
      contentLenStr = request.headers().get("x-amz-decoded-content-length");
    } else if (request.headers().get("Content-Length") != null) {
      contentLenStr = request.headers().get("Content-Length");
    }
    String accessLog = String.format("[ACCESSLOG] %s Request:%s - Status:%d "
            + "- Request ContentLength:%s - Elapsed(ms):%d",
        (opType == null ? "" : opType), request.uri(), response.status().code(),
        contentLenStr, stopWatch.elapsed(TimeUnit.MILLISECONDS));
    if (LOG.isDebugEnabled()) {
      String requestHeaders = request.headers().entries().stream()
          .map(x -> x.getKey() + ":" + x.getValue())
          .collect(Collectors.joining("\n"));
      String responseHeaders = response.headers().entries().stream()
          .map(x -> x.getKey() + ":" + x.getValue())
          .collect(Collectors.joining("\n"));
      String moreInfoStr = String.format("%n[RequestHeader]:%n%s%n[ResponseHeader]:%n%s",
          requestHeaders, responseHeaders);
      LOG.debug(accessLog + " " + moreInfoStr);
    } else {
      LOG.info(accessLog);
    }
  }

  class S3NettyFutureListener implements ChannelFutureListener {
    private S3NettyHandler mHandler;

    S3NettyFutureListener(S3NettyHandler s3NettyHandler) {
      mHandler = s3NettyHandler;
    }

    @Override
    public void operationComplete(ChannelFuture future) {
      if (future.isSuccess()) {
        S3NettyHandler.logAccess(mHandler.getRequest(), mHandler.getResponse(),
            mHandler.getStopwatch(), mHandler.getS3Task() != null
                ? mHandler.getS3Task().getOPType() : S3NettyBaseTask.OpType.Unknown);
        future.channel().close();
      } else {
        Throwable cause = future.cause();
        LOG.warn("write to channel failed: {}.", cause.toString());
      }
    }
  }
}
