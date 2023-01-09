package alluxio.proxy.s3;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.util.ThreadUtils;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.eclipse.jetty.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class S3RequestServlet extends HttpServlet {
    private static final Logger LOG = LoggerFactory.getLogger(S3RequestServlet.class);
    private static S3RequestServlet mInstance = null;
    public static final String SERVICE_PREFIX = "s3";
    public static final String S3_SERVICE_PATH_PREFIX = Constants.REST_API_PREFIX + AlluxioURI.SEPARATOR + SERVICE_PREFIX;
    private static ReentrantLock mCreateInstanceLock = new ReentrantLock();
    public ConcurrentHashMap<Request, S3Handler> s3HandlerMap = new ConcurrentHashMap<>();
    public ExecutorService esLight_ = new ThreadPoolExecutor(8, 64, 0,
            TimeUnit.SECONDS, new ArrayBlockingQueue<>(64 * 1024),
            new ThreadFactoryBuilder().setNameFormat("S3-LIGHQ-%d").build());
    public ExecutorService esHeavy_ = new ThreadPoolExecutor(8, 64, 0,
            TimeUnit.SECONDS, new ArrayBlockingQueue<>(64 * 1024),
            new ThreadFactoryBuilder().setNameFormat("S3-HEAVYQ-%d").build());

    public static S3RequestServlet getInstance() {
        if (mInstance != null)
            return mInstance;
        try {
            mCreateInstanceLock.lock();
            if (mInstance != null)
                return mInstance;
            mInstance = new S3RequestServlet();
            return mInstance;
        } finally {
            mCreateInstanceLock.unlock();
        }
    }

    @Override
    public void service(HttpServletRequest request,
                        HttpServletResponse response) throws ServletException, IOException {
        String target = request.getRequestURI();
        if (!target.startsWith(S3_SERVICE_PATH_PREFIX)) {
            return;
        }
        try {
            S3Handler s3Handler = S3Handler.createHandler(target, request, response);
            s3HandlerMap.put((Request) request, s3Handler);
            // Handle request async
            if (Configuration.getBoolean(PropertyKey.PROXY_S3_ASYNC_PROCESSING_ENABLED)) {
                S3BaseTask.OpTag opTag = s3Handler.getS3Task().mOPType.getOpTag();
                ExecutorService es = opTag == S3BaseTask.OpTag.LIGHT ? esLight_ : esHeavy_;

                final AsyncContext asyncCtx = request.startAsync();
                final S3Handler s3HandlerAsync = s3Handler;
                es.submit(() -> {
                    try {
                        doService(s3HandlerAsync);
                    } catch (Throwable th) {
                        try {
                            ((HttpServletResponse) asyncCtx.getResponse()).sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                        } catch (Throwable sendErrorEx) {
                            LOG.error("Unexpected exception for {}/{}. {}", s3HandlerAsync.getBucket(),
                                    s3HandlerAsync.getObject(), ThreadUtils.formatStackTrace(th));
                        }
                    } finally {
                        asyncCtx.complete();
                    }
                });
            }
            // Handle request in current context
            else {
                serveRequest(s3Handler);
            }
        } catch ( Throwable th ) {
            Response errorResponse = S3ErrorResponse.createErrorResponse(th, "");
            S3Handler.processResponse(response, errorResponse);
        }
    }

    public void serveRequest(S3Handler s3Handler) throws IOException {
        if (s3Handler.getS3Task().getOPType() == S3BaseTask.OpType.CompleteMultipartUpload) {
            s3Handler.getS3Task().handleTaskAsync();
            return;
        }
        Response resp = s3Handler.getS3Task().continueTask();
        S3Handler.processResponse(s3Handler.getServletResponse(), resp);
    }

    public void doService(S3Handler s3Handler) throws IOException {
        try {
            serveRequest(s3Handler);
        } catch ( Throwable th ) {
            Response errorResponse = S3ErrorResponse.createErrorResponse(th, "");
            S3Handler.processResponse(s3Handler.getServletResponse(), errorResponse);
        }
    }
}

