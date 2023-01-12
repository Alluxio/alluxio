package alluxio.proxy.s3;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.Bits;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.PMode;
import alluxio.grpc.XAttrPropagationStrategy;
import alluxio.master.audit.AsyncUserAccessAuditLogWriter;
import alluxio.util.CommonUtils;
import alluxio.util.ThreadUtils;
import alluxio.web.ProxyWebServer;

import com.google.common.base.Stopwatch;
import org.eclipse.jetty.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

/**
 *
 */
public class S3Handler {
  public static final boolean BUCKET_NAMING_RESTRICTION_ENABLED =
          Configuration.getBoolean(PropertyKey.PROXY_S3_BUCKET_NAMING_RESTRICTIONS_ENABLED);
  public static final int MAX_HEADER_METADATA_SIZE =
          (int) Configuration.getBytes(PropertyKey.PROXY_S3_METADATA_HEADER_MAX_SIZE);
  public static final boolean MULTIPART_CLEANER_ENABLED =
          Configuration.getBoolean(PropertyKey.PROXY_S3_MULTIPART_UPLOAD_CLEANER_ENABLED);
  // https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
  // - Undocumented edge-case, no adjacent periods with hyphens, i.e: '.-' or '-.'
  public static final Pattern BUCKET_ADJACENT_DOTS_DASHES_PATTERN = Pattern.compile("([-\\.]{2})");
  public static final Pattern BUCKET_INVALIDATION_PREFIX_PATTERN = Pattern.compile("^xn--.*");
  public static final Pattern BUCKET_INVALID_SUFFIX_PATTERN = Pattern.compile(".*-s3alias$");
  public static final Pattern BUCKET_VALID_NAME_PATTERN =
          Pattern.compile("[a-z0-9][a-z0-9\\.-]{1,61}[a-z0-9]");
  public static final Pattern BASE_PATH_PATTERN =
          Pattern.compile("^" + S3RequestServlet.S3_SERVICE_PATH_PREFIX + "$");
  public static final Pattern BUCKET_PATH_PATTERN =
          Pattern.compile("^" + S3RequestServlet.S3_SERVICE_PATH_PREFIX + "/[^/]*$");
  public static final Pattern OBJECT_PATH_PATTERN =
          Pattern.compile("^" + S3RequestServlet.S3_SERVICE_PATH_PREFIX + "/[^/]*/.*$");
  private static final Logger LOG = LoggerFactory.getLogger(S3Handler.class);
  private static final ThreadLocal<byte[]> TLS_BYTES =
          ThreadLocal.withInitial(() -> new byte[8 * 1024]);
  private final String mBucket;
  private final String mObject;
  private final HttpServletRequest mServletRequest;
  private final HttpServletResponse mServletResponse;
  public AsyncUserAccessAuditLogWriter mAsyncAuditLogWriter;
  String[] mUnsupportedSubResources = {"acl", "policy", "versioning", "cors",
    "encryption", "intelligent-tiering", "inventory", "lifecycle",
    "metrics", "ownershipControls", "replication", "website", "accelerate",
    "location", "logging", "metrics", "notification", "ownershipControls",
    "policyStatus", "requestPayment", "attributes", "legal-hold", "object-lock",
    "retention", "torrent", "publicAccessBlock", "restore", "select"};
  Set<String> mUnsupportedSubResourcesSet = new HashSet<>(Arrays.asList(mUnsupportedSubResources));
  Map<String, String> mAmzHeaderMap = new HashMap<>();
  Request mBaseRequest;
  private Stopwatch mStopwatch;
  private String mUser;
  private S3BaseTask mS3Task;
  private FileSystem mMetaFS;

  /**
   * S3Handler Constructor.
   * @param bucket
   * @param object
   * @param request
   * @param response
   */
  public S3Handler(String bucket, String object,
                   HttpServletRequest request, HttpServletResponse response) {
    mBucket = bucket;
    mObject = object;
    mServletRequest = request;
    mServletResponse = response;
  }

  /**
   * Create a S3Handler based on the incoming Request.
   * @param path
   * @param request
   * @param response
   * @return A S3Handler
   * @throws Exception
   *
   */
  public static S3Handler createHandler(String path,
                                        HttpServletRequest request,
                                        HttpServletResponse response) throws Exception {
    Stopwatch stopwatch = Stopwatch.createStarted();
    Matcher bucketMatcher = BUCKET_PATH_PATTERN.matcher(path);
    Matcher objectMatcher = OBJECT_PATH_PATTERN.matcher(path);

    String pathStr = path;
    String bucket = null;
    String object = null;
    S3Handler handler = null;
    try {
      if (bucketMatcher.matches()) {
        pathStr = path.substring(S3RequestServlet.S3_SERVICE_PATH_PREFIX.length() + 1);
        bucket = URLDecoder.decode(pathStr, "UTF-8");
      } else if (objectMatcher.matches()) {
        pathStr = path.substring(S3RequestServlet.S3_SERVICE_PATH_PREFIX.length() + 1);
        bucket = URLDecoder.decode(
          pathStr.substring(0, pathStr.indexOf(AlluxioURI.SEPARATOR)), "UTF-8");
        object = URLDecoder.decode(
          pathStr.substring(pathStr.indexOf(AlluxioURI.SEPARATOR) + 1), "UTF-8");
      }
      handler = new S3Handler(bucket, object, request, response);
      handler.setStopwatch(stopwatch);
      handler.init();
      S3BaseTask task = null;
      if (object != null && !object.isEmpty()) {
        task = S3ObjectTask.Factory.create(handler);
      } else {
        task = S3BucketTask.Factory.create(handler);
      }
      handler.setS3Task(task);
      return handler;
    } catch (Exception ex) {
      LOG.error("Exception during create s3handler:", ThreadUtils.formatStackTrace(ex));
      throw ex;
    }
  }

  /**
   * Process the response returned from S3Task core logic to write to downstream.
   * @param servletResponse
   * @param response
   * @throws IOException
   */
  public static void processResponse(HttpServletResponse servletResponse,
                                     Response response) throws IOException {
    try {
      // Status
      servletResponse.setStatus(response.getStatus());
      // Headers
      final MultivaluedMap<String, String> headers = response.getStringHeaders();
      for (final Map.Entry<String, List<String>> e : headers.entrySet()) {
        final Iterator<String> it = e.getValue().iterator();
        if (!it.hasNext()) {
          continue;
        }
        final String header = e.getKey();
        if (servletResponse.containsHeader(header)) {
          // replace any headers previously set with values from Jersey container response.
          servletResponse.setHeader(header, it.next());
        }
        while (it.hasNext()) {
          servletResponse.addHeader(header, it.next());
        }
      }
      // Entity
      if (response.hasEntity()) {
        Object entity = response.getEntity();
        if (entity instanceof InputStream) {
          InputStream is = (InputStream) entity;
          byte[] bytesArray = TLS_BYTES.get();
          int read;
          while ((read = is.read(bytesArray)) != -1) {
            servletResponse.getOutputStream().write(bytesArray, 0, read);
          }
        } else {
          String contentStr = entity.toString();
          int contentLen = contentStr.length();
          servletResponse.setContentLength(contentLen);
          servletResponse.getOutputStream().write(contentStr.getBytes());
        }
      }
    } finally {
      response.close();
    }
  }

  /**
   * Initialize the S3Handler object in preparation for handling the request.
   * @throws Exception
   */
  public void init() throws Exception {
    // Do Authentication of the request.
    doAuthorization();
    // Extract x-amz- headers.
    extractAMZHeaders();
    // Reject unsupported subresources.
    rejectUnsupportedResources();
    // Init utils
    mMetaFS = ProxyWebServer.mFileSystem;
    mAsyncAuditLogWriter = ProxyWebServer.mAsyncAuditLogWriter;
    // Initiate the S3 API metadata directories
    if (!mMetaFS.exists(new AlluxioURI(S3RestUtils.MULTIPART_UPLOADS_METADATA_DIR))) {
      mMetaFS.createDirectory(new AlluxioURI(S3RestUtils.MULTIPART_UPLOADS_METADATA_DIR),
          CreateDirectoryPOptions.newBuilder()
            .setRecursive(true)
            .setMode(PMode.newBuilder()
              .setOwnerBits(Bits.ALL).setGroupBits(Bits.ALL)
              .setOtherBits(Bits.NONE)
              .build())
            .setWriteType(S3RestUtils.getS3WriteType())
            .setXattrPropStrat(XAttrPropagationStrategy.LEAF_NODE)
            .build());
    }
  }

  /**
   * get S3Task of this S3Handler.
   * @return S3BaseTask
   */
  public S3BaseTask getS3Task() {
    return mS3Task;
  }

  /**
   * set S3Task for this S3Handler.
   * @param task
   */
  public void setS3Task(S3BaseTask task) {
    mS3Task = task;
  }

  /**
   * get HTTP verb of this request.
   * @return HTTP Verb
   */
  public String getHTTPVerb() {
    return mServletRequest.getMethod();
  }

  /**
   * get specified HTTP header value of this request.
   * @param headerName
   * @return header value
   */
  public String getHeader(String headerName) {
    return mServletRequest.getHeader(headerName);
  }

  /**
   * get specified HTTP header with a default if not exist.
   * @param headerName
   * @param defaultHeaderValue
   * @return header value
   */
  public String getHeaderOrDefault(String headerName, String defaultHeaderValue) {
    String headerVal = mServletRequest.getHeader(headerName);
    if (headerVal == null) {
      headerVal = defaultHeaderValue;
    }
    return headerVal;
  }

  /**
   * get HttpServletResponse of this request.
   * @return HttpServletResponse
   */
  public HttpServletResponse getServletResponse() {
    return mServletResponse;
  }

  /**
   * get HttpServletRequest of this request.
   * @return HttpServletRequest
   */
  public HttpServletRequest getServletRequest() {
    return mServletRequest;
  }

  /**
   * retrieve given query parameter value.
   * @param queryParam
   * @return query parameter value
   */
  public String getQueryParameter(String queryParam) {
    return mServletRequest.getParameter(queryParam);
  }

  /**
   * retrieve inputstream from incoming request.
   * @return ServletInputStream
   */
  public ServletInputStream getInputStream() throws IOException {
    return mServletRequest.getInputStream();
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
      auditContext.setUgi(ugi)
          .setCommand(command)
          .setIp(String.format("%s:%s",
              mServletRequest.getRemoteAddr(), mServletRequest.getRemotePort()))
          .setBucket(bucket)
          .setObject(object)
          .setAllowed(true)
          .setSucceeded(true)
          .setCreationTimeNs(System.nanoTime());
    }
    return auditContext;
  }

  /**
   * Utility function to dump a collection into a string.
   * @param prefix
   * @param collection
   * @return result string
   */
  public String printCollection(String prefix, Collection<? extends Object> collection) {
    StringBuilder sb = new StringBuilder(prefix + ":[");
    Iterator<? extends Object> it = collection.iterator();
    while (it.hasNext()) {
      sb.append(it.next().toString());
      if (it.hasNext()) {
        sb.append(",");
      }
    }
    sb.append("]");
    return sb.toString();
  }

  /**
   * Utility function to dump a map into a string.
   * @param prefix
   * @param map
   * @return result string
   */
  public String printMap(String prefix, Map<? extends Object, ? extends Object> map) {
    StringBuilder sb = new StringBuilder(prefix + ":[");
    Iterator<? extends Map.Entry<?, ?>> it = map.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<?, ?> entry = it.next();
      sb.append(entry.getKey().toString() + ":" + entry.getValue().toString());
      if (it.hasNext()) {
        sb.append(",");
      }
    }
    sb.append("]");
    return sb.toString();
  }

  /**
   * Utility function to help extract x-amz- headers from request.
   */
  public void extractAMZHeaders() {
    java.util.Enumeration<String> headerNamesIt = mServletRequest.getHeaderNames();
    while (headerNamesIt.hasMoreElements()) {
      String header = headerNamesIt.nextElement();
      mAmzHeaderMap.putIfAbsent(header, mServletRequest.getHeader(header));
    }
  }

  /**
   * Reject unsupported request from the given subresources from request.
   * @throws S3Exception
   */
  public void rejectUnsupportedResources() throws S3Exception {
    java.util.Enumeration<String> parameterNamesIt = mServletRequest.getParameterNames();
    while (parameterNamesIt.hasMoreElements()) {
      if (mUnsupportedSubResourcesSet.contains(parameterNamesIt.nextElement())) {
        throw new S3Exception(S3Constants.EMPTY, S3ErrorCode.NOT_IMPLEMENTED);
      }
    }
  }

  /**
   * Do S3 request authorization.
   * @throws Exception
   */
  public void doAuthorization() throws Exception {
    try {
      String authorization = mServletRequest.getHeader("Authorization");
      String user = S3RestUtils.getUser(authorization, mServletRequest);
      // replace the authorization header value to user
      LOG.debug("request origin Authorization Header is: {}, new user header is: {}",
          authorization, user);
      mUser = user;
    } catch (Exception e) {
      LOG.warn("exception happened in Authentication.");
      throw e;
    }
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
   * Get system user FileSystem object.
   * @return FileSystem object
   */
  public FileSystem getMetaFS() {
    return mMetaFS;
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
}
