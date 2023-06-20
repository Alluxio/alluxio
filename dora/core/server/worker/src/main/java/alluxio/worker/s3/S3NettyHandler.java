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
import alluxio.client.file.FileSystem;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.audit.AsyncUserAccessAuditLogWriter;
import alluxio.s3.S3AuditContext;
import alluxio.s3.S3Constants;
import alluxio.s3.S3ErrorCode;
import alluxio.s3.S3Exception;
import alluxio.security.User;
import alluxio.util.CommonUtils;
import alluxio.util.ThreadUtils;
import alluxio.worker.dora.DoraWorker;
import alluxio.worker.dora.PagedDoraWorker;
import javax.annotation.Nullable;
import javax.security.auth.Subject;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3NettyHandler {
  private static final Logger LOG = LoggerFactory.getLogger(S3NettyHandler.class);
  private String mUser;
  private final String mBucket;
  private final String mObject;
  private final FullHttpRequest mRequest;
  private final ChannelHandlerContext mContext;
  private final QueryStringDecoder mQueryDecoder;
  private S3NettyBaseTask mS3Task;
  private FileSystem mFsClient;
  private DoraWorker mDoraWorker;
  public AsyncUserAccessAuditLogWriter mAsyncAuditLogWriter;

  public static final Pattern BUCKET_PATH_PATTERN = Pattern.compile("^" + "/[^/]*$");
  public static final Pattern OBJECT_PATH_PATTERN = Pattern.compile("^" + "/[^/]*/.*$");
  String[] mUnsupportedSubResources = {"acl", "policy", "versioning", "cors",
      "encryption", "intelligent-tiering", "inventory", "lifecycle",
      "metrics", "ownershipControls", "replication", "website", "accelerate",
      "location", "logging", "metrics", "notification", "ownershipControls",
      "policyStatus", "requestPayment", "attributes", "legal-hold", "object-lock",
      "retention", "torrent", "publicAccessBlock", "restore", "select"};
  Set<String> mUnsupportedSubResourcesSet = new HashSet<>(Arrays.asList(mUnsupportedSubResources));
  Map<String, String> mAmzHeaderMap = new HashMap<>();

  public S3NettyHandler(String bucket, String object, FullHttpRequest request,
                        ChannelHandlerContext ctx, FileSystem fileSystem,
                        DoraWorker doraWorker) {
    mBucket = bucket;
    mObject = object;
    mRequest = request;
    mContext = ctx;
    mFsClient = fileSystem;
    mDoraWorker = doraWorker;
    mQueryDecoder = new QueryStringDecoder(request.uri());
  }

  /**
   * Create a S3Handler based on the incoming Request.
   * @param context
   * @param request
   * @param fileSystem
   * @return A S3Handler
   * @throws Exception
   *
   */
  public static S3NettyHandler createHandler(ChannelHandlerContext context,
                                             FullHttpRequest request,
                                             FileSystem fileSystem,
                                             DoraWorker doraWorker) throws Exception {
    String path = request.uri();
    Matcher bucketMatcher = BUCKET_PATH_PATTERN.matcher(path);
    Matcher objectMatcher = OBJECT_PATH_PATTERN.matcher(path);
    String pathStr = path;
    String bucket = null;
    String object = null;
    S3NettyHandler handler = null;
    boolean keepAlive = HttpUtil.isKeepAlive(request);
    try {
      if (bucketMatcher.matches()) {
        pathStr = path.substring(1);
        bucket = URLDecoder.decode(pathStr, "UTF-8");
      } else if (objectMatcher.matches()) {
        pathStr = path.substring(1);
        bucket = URLDecoder.decode(
            pathStr.substring(0, pathStr.indexOf(AlluxioURI.SEPARATOR)), "UTF-8");
        object = URLDecoder.decode(
            pathStr.substring(pathStr.indexOf(AlluxioURI.SEPARATOR) + 1), "UTF-8");
      }
      handler = new S3NettyHandler(bucket, object, request, context, fileSystem, doraWorker);
//      handler.setStopwatch(stopwatch);
      handler.init();
      S3NettyBaseTask task = null;
      if (object != null && !object.isEmpty()) {
        task = S3NettyObjectTask.Factory.create(handler);
//      } else {
//        task = S3NettyBucketTask.Factory.create(handler);
      }
      handler.setS3Task(task);
      return handler;
    } catch (Exception ex) {
      LOG.error("Exception during create s3handler:{}", ThreadUtils.formatStackTrace(ex));
      throw ex;
    }
  }

  /**
   * Initialize the S3Handler object in preparation for handling the request.
   * @throws Exception
   */
  public void init() throws Exception {
    // Do Authentication of the request.
//    doAuthentication();
    // Extract x-amz- headers.
    extractAMZHeaders();
    // Reject unsupported subresources.
    rejectUnsupportedResources();
    // Init utils
//    ServletContext context = getServletContext();
//    mMetaFS = (FileSystem) context.getAttribute(ProxyWebServer.FILE_SYSTEM_SERVLET_RESOURCE_KEY);
//    mAsyncAuditLogWriter = (AsyncUserAccessAuditLogWriter) context.getAttribute(
//        ProxyWebServer.ALLUXIO_PROXY_AUDIT_LOG_WRITER_KEY);
    // Initiate the S3 API metadata directories
//    if (!mMetaFS.exists(new AlluxioURI(S3RestUtils.MULTIPART_UPLOADS_METADATA_DIR))) {
//      mMetaFS.createDirectory(new AlluxioURI(S3RestUtils.MULTIPART_UPLOADS_METADATA_DIR),
//          CreateDirectoryPOptions.newBuilder()
//              .setRecursive(true)
//              .setMode(PMode.newBuilder()
//                  .setOwnerBits(Bits.ALL).setGroupBits(Bits.ALL)
//                  .setOtherBits(Bits.NONE)
//                  .build())
//              .setWriteType(S3RestUtils.getS3WriteType())
//              .setXattrPropStrat(XAttrPropagationStrategy.LEAF_NODE)
//              .build());
//    }
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
      if (mUnsupportedSubResourcesSet.contains(parameter)) {
        throw new S3Exception(S3Constants.EMPTY, S3ErrorCode.NOT_IMPLEMENTED);
      }
    }
  }

//  /**
//   * Do S3 request authentication.
//   * @throws Exception
//   */
//  public void doAuthentication() throws Exception {
//    try {
//      String authorization = mServletRequest.getHeader("Authorization");
//      String user = S3RestUtils.getUser(authorization, mServletRequest);
//      // replace the authorization header value to user
//      LOG.debug("request origin Authorization Header is: {}, new user header is: {}",
//          authorization, user);
//      mUser = user;
//    } catch (Exception e) {
//      LOG.warn("exception happened in Authentication.");
//      throw e;
//    }
//  }

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
      auditContext.setUgi(ugi)
          .setCommand(command)
//          .setIp(String.format("%s:%s",
//              mServletRequest.getRemoteAddr(), mServletRequest.getRemotePort()))
          .setBucket(bucket)
          .setObject(object)
          .setAllowed(true)
          .setSucceeded(true)
          .setCreationTimeNs(System.nanoTime());
    }
    return auditContext;
  }

  /**
   * set S3Task for this S3Handler.
   * @param task
   */
  public void setS3Task(S3NettyBaseTask task) {
    mS3Task = task;
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
  public List<String> getQueryParameter(String queryParam) {
    return mQueryDecoder.parameters().get(queryParam);
  }

  /**
   * @param user the {@link Subject} name of the filesystem user
   * @return A {@link FileSystem} with the subject set to the provided user
   */
  public FileSystem createFileSystemForUser(String user) {
    if (user == null) {
      // Used to return the top-level FileSystem view when not using Authentication
      return mFsClient;
    }

    final Subject subject = new Subject();
    subject.getPrincipals().add(new User(user));
    // Use local conf to create filesystem rather than fs.getConf()
    // due to fs conf will be changed by merged cluster conf.
    return FileSystem.Factory.get(subject, Configuration.global());
  }
}
