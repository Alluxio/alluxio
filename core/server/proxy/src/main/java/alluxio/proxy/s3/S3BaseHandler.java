package alluxio.proxy.s3;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import alluxio.AlluxioURI;
import com.google.common.base.Preconditions;
import org.eclipse.jetty.server.Request;

import java.util.*;

public class S3BaseHandler {

    String[] unsupportedSubResources_ = {
            "acl", "policy"
    };

    String mBucket;
    String mObject;

    Set<String> unsupportedSubResourcesSet_ = new HashSet<>(Arrays.asList(unsupportedSubResources_));
    Map<String, String> amzHeaderMap_ = new HashMap<>();
    Request mBaseRequest;
    HttpServletRequest mServletRequest;
    HttpServletResponse mServletResponse;
    private S3BaseTask mS3Task;


    public S3BaseHandler(String bucket, String object,
                         Request baseRequest,
                         HttpServletRequest request,
                         HttpServletResponse response) {
        mBucket = bucket;
        mObject = object;
        mBaseRequest = baseRequest;
        mServletRequest = request;
        mServletResponse = response;
    }

    public void setS3Task(S3BaseTask task) {
        mS3Task = task;
    }

    public S3BaseTask getS3Task(S3BaseTask task) {
        return mS3Task;
    }

    public static S3BaseHandler createHandler(String path, Request baseRequest,
                                              HttpServletRequest request,
                                              HttpServletResponse response) {
        String pathStr = path.substring(S3RequstHandler.S3_SERVICE_PATH_PREFIX.length() + 1); // substring the prefix + leading "/" character
        final String bucket = pathStr.substring(0, pathStr.indexOf(AlluxioURI.SEPARATOR));
        final String object = pathStr.substring(pathStr.indexOf(AlluxioURI.SEPARATOR) + 1);
        Preconditions.checkNotNull(bucket, "required 'bucket' parameter is missing");
        S3BaseHandler handler = new S3BaseHandler(bucket, object, baseRequest, request, response);
        handler.init();
        S3BaseTask task = null;
        if (object != null && !object.isEmpty()) {
            task = S3BucketTask.createTask(bucket, handler);
        } else {
            task = S3ObjectTask.createTask(bucket, object, handler);
        }
        handler.setS3Task(task);
        return handler;

    }

    public void init() {
        try {
            extractAMZHeaders();
            rejectUnsupportedResources();
        } catch (Throwable th) {

        }
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

    public String getQueryParameter(String queryParam) {
        return mServletRequest.getParameter(queryParam);
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
                throw new S3Exception("", S3ErrorCode.INTERNAL_ERROR);
            }
        }
    }
}
