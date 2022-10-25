package alluxio.proxy.s3;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.conf.InstancedConfiguration;
import alluxio.exception.runtime.UnimplementedRuntimeException;
import alluxio.master.audit.AsyncUserAccessAuditLogWriter;
import org.eclipse.jetty.server.HttpConnection;
import org.eclipse.jetty.server.HttpInput;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;


public class S3RequstHandler extends AbstractHandler {

    private static final Logger LOG = LoggerFactory.getLogger(S3RestServiceHandler.class);

    private static S3RequstHandler mInstance = null;
    private static ReentrantLock mCreateInstanceLock = new ReentrantLock();

    public static final String SERVICE_PREFIX = "s3";
    public static final String S3_SERVICE_PATH_PREFIX = Constants.REST_API_PREFIX + AlluxioURI.SEPARATOR + SERVICE_PREFIX;

    /* Bucket is the first component in the URL path. */
    public static final String BUCKET_PARAM = "{bucket}/";
    /* Object is after bucket in the URL path */
    public static final String OBJECT_PARAM = "{bucket}/{object:.+}";

//    private final FileSystem mMetaFS;
//    private final InstancedConfiguration mSConf;

    @Context
    private ContainerRequestContext mRequestContext;
    @Context
    HttpServletRequest mServletRequest;

    private AsyncUserAccessAuditLogWriter mAsyncAuditLogWriter;

//    private final boolean mBucketNamingRestrictionsEnabled;
//    private final int mMaxHeaderMetadataSize; // 0 means disabled
//    private final boolean mMultipartCleanerEnabled;
//
//    private final Pattern mBucketAdjacentDotsDashesPattern;
//    private final Pattern mBucketInvalidPrefixPattern;
//    private final Pattern mBucketInvalidSuffixPattern;
//    private final Pattern mBucketValidNamePattern;

    public static S3RequstHandler getInstance() {
        if (mInstance != null)
            return mInstance;
        try {
            mCreateInstanceLock.lock();
            if (mInstance != null)
                return mInstance;
            mInstance = new S3RequstHandler(); // add static fields
            return mInstance;
        } finally {
            mCreateInstanceLock.unlock();
        }
    }

    @Override
    public void handle(String target,
                       Request baseRequest,
                       HttpServletRequest request,
                       HttpServletResponse response) throws IOException, ServletException {
        // determine bucket / object
        if (!target.startsWith(S3_SERVICE_PATH_PREFIX)) {
            return;
        }
        S3BaseHandler s3Handler = S3BaseHandler.createHandler(target, baseRequest, request, response);
//        s3Handler.handleRequest();
    }

    public void handleRequest() {
        throw new UnsupportedOperationException();
    }
}
