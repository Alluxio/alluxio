package alluxio.proxy.s3;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.runtime.UnimplementedRuntimeException;
import alluxio.grpc.Bits;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.PMode;
import alluxio.grpc.XAttrPropagationStrategy;
import alluxio.master.audit.AsyncUserAccessAuditLogWriter;
import alluxio.web.ProxyWebServer;
import com.google.common.base.Stopwatch;
import org.eclipse.jetty.server.HttpConnection;
import org.eclipse.jetty.server.HttpInput;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;


public class S3RequstHandler extends HttpServlet {

    private static final Logger LOG = LoggerFactory.getLogger(S3RequstHandler.class);
    private static S3RequstHandler mInstance = null;
    private static ReentrantLock mCreateInstanceLock = new ReentrantLock();

    public static final String SERVICE_PREFIX = "s3";
    public static final String S3_SERVICE_PATH_PREFIX = Constants.REST_API_PREFIX + AlluxioURI.SEPARATOR + SERVICE_PREFIX;

    @Context
    private ContainerRequestContext mRequestContext;
    @Context
    HttpServletRequest mServletRequest;


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
    public void service(HttpServletRequest request,
                        HttpServletResponse response)  throws ServletException, IOException {
        Stopwatch stopWatch = Stopwatch.createStarted();
        String target = request.getRequestURI();
        if (!target.startsWith(S3_SERVICE_PATH_PREFIX)) {
            return;
        }
        S3BaseHandler s3Handler = S3BaseTask.createHandler(target, request, response);
        Response resp = s3Handler.getS3Task().handleTask();
        s3Handler.processResponse(resp);
        ProxyWebServer.logAccess(request, response, stopWatch);
    }

}
