package alluxio.proxy.s3;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.web.ProxyWebServer;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;


public class S3RequestServlet extends HttpServlet {

    private static final Logger LOG = LoggerFactory.getLogger(S3RequestServlet.class);
    private static S3RequestServlet mInstance = null;
    private static ReentrantLock mCreateInstanceLock = new ReentrantLock();

    public static final String SERVICE_PREFIX = "s3";
    public static final String S3_SERVICE_PATH_PREFIX = Constants.REST_API_PREFIX + AlluxioURI.SEPARATOR + SERVICE_PREFIX;

    @Context
    private ContainerRequestContext mRequestContext;
    @Context
    HttpServletRequest mServletRequest;


    public static S3RequestServlet getInstance() {
        if (mInstance != null)
            return mInstance;
        try {
            mCreateInstanceLock.lock();
            if (mInstance != null)
                return mInstance;
            mInstance = new S3RequestServlet(); // add static fields
            return mInstance;
        } finally {
            mCreateInstanceLock.unlock();
        }
    }

    public static ExecutorService esLight_ = new ThreadPoolExecutor(8, 64, 0,
                                 TimeUnit.SECONDS, new ArrayBlockingQueue<>(64 * 1024),
            new ThreadFactoryBuilder().setNameFormat("S3-LIGHQ-%d").build());
    public static ExecutorService esHeavy_ = new ThreadPoolExecutor(8, 64, 0,
            TimeUnit.SECONDS, new ArrayBlockingQueue<>(64 * 1024),
            new ThreadFactoryBuilder().setNameFormat("S3-HEAVYQ-%d").build());

    @Override
    public void service(HttpServletRequest request,
                        HttpServletResponse response)  throws ServletException, IOException {
        Stopwatch stopWatch = Stopwatch.createStarted();
        String target = request.getRequestURI();
        if (!target.startsWith(S3_SERVICE_PATH_PREFIX)) {
            return;
        }
        Response resp = null;
        S3BaseTask.OpType opType = S3BaseTask.OpType.Unknown;
        try {
            S3Handler s3Handler = S3Handler.createHandler(target, request, response);
            opType = s3Handler.getS3Task().mOPType;
            if (s3Handler.getS3Task().mOPType == S3BaseTask.OpType.CompleteMultipartUpload) {
                s3Handler.getS3Task().handleTaskAsync();
                return;
            }
            resp = s3Handler.getS3Task().continueTask();
        } catch (S3Exception e) {
            resp = S3ErrorResponse.createErrorResponse(e, "");
        }
        S3Handler.processResponse(response, resp);
        ProxyWebServer.logAccess(request, response, stopWatch, opType);
    }

}

