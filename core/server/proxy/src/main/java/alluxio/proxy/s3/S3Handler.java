package alluxio.proxy.s3;

import javax.annotation.Nullable;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

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
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class S3Handler {
    private static final Logger LOG = LoggerFactory.getLogger(S3Handler.class);

    String[] unsupportedSubResources_ = {
            "acl", "policy", "versioning", "cors", "encryption", "intelligent-tiering", "inventory",
            "lifecycle", "metrics", "ownershipControls", "replication", "website","accelerate", "location",
            "logging", "metrics", "notification", "ownershipControls", "policyStatus", "requestPayment",
            "attributes", "legal-hold", "object-lock", "retention", "torrent", "publicAccessBlock","restore",
            "select"
    };

    private Stopwatch mStopwatch;
    private String mBucket;
    private String mObject;
    private String mUser;
    Set<String> unsupportedSubResourcesSet_ = new HashSet<>(Arrays.asList(unsupportedSubResources_));
    Map<String, String> amzHeaderMap_ = new HashMap<>();
    Request mBaseRequest;
    private HttpServletRequest mServletRequest;
    private HttpServletResponse mServletResponse;
    private S3BaseTask mS3Task;

    public static final boolean mBucketNamingRestrictionsEnabled = Configuration.getBoolean(
        PropertyKey.PROXY_S3_BUCKET_NAMING_RESTRICTIONS_ENABLED);
    public static final int mMaxHeaderMetadataSize = (int) Configuration.getBytes(
        PropertyKey.PROXY_S3_METADATA_HEADER_MAX_SIZE);
    public static final boolean mMultipartCleanerEnabled = Configuration.getBoolean(
        PropertyKey.PROXY_S3_MULTIPART_UPLOAD_CLEANER_ENABLED);

    // https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
    // - Undocumented edge-case, no adjacent periods with hyphens, i.e: '.-' or '-.'
    public static final Pattern mBucketAdjacentDotsDashesPattern = Pattern.compile("([-\\.]{2})");
    public static final Pattern mBucketInvalidPrefixPattern = Pattern.compile("^xn--.*");
    public static final Pattern mBucketInvalidSuffixPattern = Pattern.compile(".*-s3alias$");
    public static final Pattern mBucketValidNamePattern = Pattern.compile("[a-z0-9][a-z0-9\\.-]{1,61}[a-z0-9]");
    public static final Pattern mBasePathPattern = Pattern.compile("^" + S3RequestServlet.S3_SERVICE_PATH_PREFIX + "$");
    public static final Pattern mBucketPathPattern = Pattern.compile("^" + S3RequestServlet.S3_SERVICE_PATH_PREFIX + "/[^/]*$");
    public static final Pattern mObjectPathPattern = Pattern.compile("^" + S3RequestServlet.S3_SERVICE_PATH_PREFIX + "/[^/]*/.*$");
    private FileSystem mMetaFS;
    public AsyncUserAccessAuditLogWriter mAsyncAuditLogWriter;
    private static final ThreadLocal<byte[]> tlsBytes_ =
            ThreadLocal.withInitial(() -> new byte[8*1024]);

    public S3Handler(String bucket, String object,
                     HttpServletRequest request,
                     HttpServletResponse response) {
        mBucket = bucket;
        mObject = object;
        mServletRequest = request;
        mServletResponse = response;
    }

    public void init() throws S3Exception{
        try {
            // Do Authentication of the request.
            doAuthorization();
            // Extract x-amz- headers.
            extractAMZHeaders();
            // Reject unsupported subresources.
            rejectUnsupportedResources();
            // Init utils
            mMetaFS = ProxyWebServer.mFileSystem;
            mAsyncAuditLogWriter = ProxyWebServer.mAsyncAuditLogWriter;
            // Initiate the S3 API metadata directories -> TODO this should be init once instead every time
            if (!mMetaFS.exists(new AlluxioURI(S3RestUtils.MULTIPART_UPLOADS_METADATA_DIR))) {
                mMetaFS.createDirectory(
                        new AlluxioURI(S3RestUtils.MULTIPART_UPLOADS_METADATA_DIR),
                        CreateDirectoryPOptions.newBuilder()
                                .setRecursive(true)
                                .setMode(PMode.newBuilder()
                                        .setOwnerBits(Bits.ALL)
                                        .setGroupBits(Bits.ALL)
                                        .setOtherBits(Bits.NONE).build())
                                .setWriteType(S3RestUtils.getS3WriteType())
                                .setXattrPropStrat(XAttrPropagationStrategy.LEAF_NODE)
                                .build()
                );
            }
        } catch (Exception ex) {
            LOG.error(ThreadUtils.formatStackTrace(ex));
            throw S3RestUtils.toBucketS3Exception(ex, mBucket);
        }
    }

    public void setS3Task(S3BaseTask task) { mS3Task = task; }

    public S3BaseTask getS3Task() {
        return mS3Task;
    }

    public static S3Handler createHandler(String path,
                                       HttpServletRequest request,
                                       HttpServletResponse response) throws S3Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Matcher baseMatcher = mBasePathPattern.matcher(path);
        Matcher bucketMatcher = mBucketPathPattern.matcher(path);
        Matcher objectMatcher = mObjectPathPattern.matcher(path);

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
                bucket = URLDecoder.decode(pathStr.substring(0, pathStr.indexOf(AlluxioURI.SEPARATOR)), "UTF-8");
                object = URLDecoder.decode(pathStr.substring(pathStr.indexOf(AlluxioURI.SEPARATOR) + 1), "UTF-8");
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
            throw S3RestUtils.toObjectS3Exception(ex, "");
        }
    }


    public String getHTTPVerb() {
        return mServletRequest.getMethod();
    }

    public String getHeader(String headerName) {
        return mServletRequest.getHeader(headerName);
    }

    public String getHeaderOrDefault(String headerName, String defaultHeaderValue) {
        String headerVal = mServletRequest.getHeader(headerName);
        if (headerVal == null) {
            headerVal = defaultHeaderValue;
        }
        return headerVal;
    }

    public HttpServletResponse getServletResponse() {
        return mServletResponse;
    }

    public HttpServletRequest getServletRequest() {
        return mServletRequest;
    }

    public String getQueryParameter(String queryParam) {
        return mServletRequest.getParameter(queryParam);
    }

    public ServletInputStream getInputStream() throws IOException {
        return mServletRequest.getInputStream();
    }

    public static void processResponse(HttpServletResponse servletResponse, Response response)
            throws IOException {
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
                    byte[] bytesArray = tlsBytes_.get();
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

    public S3AuditContext createAuditContext(String command, String user,
                                              @Nullable String bucket, @Nullable String object) {
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
                    .setIp(String.format("%s:%s", mServletRequest.getRemoteAddr(),
                            mServletRequest.getRemotePort()))
                    .setBucket(bucket)
                    .setObject(object)
                    .setAllowed(true)
                    .setSucceeded(true)
                    .setCreationTimeNs(System.nanoTime());
        }
        return auditContext;
    }

    public String printCollection(String prefix, Collection<? extends Object> collection) {
        StringBuilder sb = new StringBuilder(prefix + ":[");
        Iterator<? extends Object> it = collection.iterator();
        while (it.hasNext()) {
            sb.append(it.next().toString());
            if (it.hasNext())
                sb.append(",");
        }
        sb.append("]");
        return sb.toString();
    }

    public String printMap(String prefix, Map<? extends Object, ? extends Object> map) {
        StringBuilder sb = new StringBuilder(prefix + ":[");
        Iterator<? extends Map.Entry<?, ?>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<?,?> entry = it.next();
            sb.append(entry.getKey().toString() + ":" + entry.getValue().toString());
            if (it.hasNext())
                sb.append(",");
        }
        sb.append("]");
        return sb.toString();
    }

    public void extractAMZHeaders() {
        java.util.Enumeration<String> headerNamesIt = mServletRequest.getHeaderNames();
        while (headerNamesIt.hasMoreElements()) {
            String header = headerNamesIt.nextElement();
            amzHeaderMap_.putIfAbsent(header, mServletRequest.getHeader(header));
        }
    }

    public void rejectUnsupportedResources() throws S3Exception {
        java.util.Enumeration<String> parameterNamesIt = mServletRequest.getParameterNames();
        while (parameterNamesIt.hasMoreElements()) {
            if (unsupportedSubResourcesSet_.contains(parameterNamesIt.nextElement())) {
                throw new S3Exception(S3Constants.EMPTY, S3ErrorCode.NOT_IMPLEMENTED);
            }
        }
    }

    public void doAuthorization() throws S3Exception {
        try {
            String authorization = mServletRequest.getHeader("Authorization");
            String user = S3RestUtils.getUser(authorization, mServletRequest);
            // replace the authorization header value to user
            LOG.debug("request origin Authorization Header is: {}, new user header is: {}",
                    authorization, user);
            mUser = user;
        } catch (Exception e) {
            LOG.warn("exception happened in Authentication:", e);
            throw new S3Exception("Authorization", S3ErrorCode.ACCESS_DENIED_ERROR);
        }
    }


    public String getUser() {
        return mUser;
    }

    public String getBucket() {
        return mBucket;
    }

    public String getObject() {
        return mObject;
    }

    public FileSystem getMetaFS() {
        return mMetaFS;
    }

    public Stopwatch getStopwatch() {
        return mStopwatch;
    }

    public void setStopwatch(Stopwatch stopwatch) {
        mStopwatch = stopwatch;
    }

}
