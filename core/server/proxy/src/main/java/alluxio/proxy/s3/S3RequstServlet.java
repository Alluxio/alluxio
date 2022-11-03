package alluxio.proxy.s3;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.web.ProxyWebServer;
import com.google.common.base.Stopwatch;
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
import java.util.concurrent.locks.ReentrantLock;


public class S3RequstServlet extends HttpServlet {

    private static final Logger LOG = LoggerFactory.getLogger(S3RequstServlet.class);
    private static S3RequstServlet mInstance = null;
    private static ReentrantLock mCreateInstanceLock = new ReentrantLock();

    public static final String SERVICE_PREFIX = "s3";
    public static final String S3_SERVICE_PATH_PREFIX = Constants.REST_API_PREFIX + AlluxioURI.SEPARATOR + SERVICE_PREFIX;

    @Context
    private ContainerRequestContext mRequestContext;
    @Context
    HttpServletRequest mServletRequest;


    public static S3RequstServlet getInstance() {
        if (mInstance != null)
            return mInstance;
        try {
            mCreateInstanceLock.lock();
            if (mInstance != null)
                return mInstance;
            mInstance = new S3RequstServlet(); // add static fields
            return mInstance;
        } finally {
            mCreateInstanceLock.unlock();
        }
    }

    @Override
    public void service(HttpServletRequest request,
                        HttpServletResponse response)  throws ServletException, IOException {
        Stopwatch stopWatch = Stopwatch.createStarted();
        String target = request.getRequestURI();
        if (!target.startsWith(S3_SERVICE_PATH_PREFIX)) {
            return;
        }
        Response resp;
        S3Handler s3Handler = null;
        try {
            s3Handler = S3Handler.createHandler(target, request, response);
            resp = s3Handler.getS3Task().continueTask();
        } catch (S3Exception e) {
            resp = S3ErrorResponse.createErrorResponse(e, "");
        }
        s3Handler.processResponse(resp);
        ProxyWebServer.logAccess(request, response, stopWatch);
    }

}
