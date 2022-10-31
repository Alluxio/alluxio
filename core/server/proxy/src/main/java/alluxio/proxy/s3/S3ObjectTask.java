package alluxio.proxy.s3;

import org.apache.commons.lang3.StringUtils;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import java.util.function.Supplier;

public class S3ObjectTask extends S3BaseTask {

    public S3ObjectTask(String bucket, String object,
                        HttpServletRequest request,
                        HttpServletResponse response) {
        super(bucket, object, request, response);
    }

    ACTION action_;

    public enum ACTION {
        ListObjects,
        NOOP
    }


    public static S3ObjectTask allocateTask(String bucket, String object,
                                            HttpServletRequest request,
                                            HttpServletResponse response) {
        S3ObjectTask task = new S3ObjectTask(bucket, object, request, response);
        ACTION action;
        switch (request.getMethod()) {
            case "GET":
                if (StringUtils.isEmpty(bucket)) {
                    action = ACTION.ListBuckets;
                } else if (request.getParameter("tagging") != null) {
                    action = ACTION.GetBucketTagging;
                } else {
                    action = S3BucketTask.ACTION.ListObjects;
                }
                break;
            case "PUT":
                if (request.getParameter("tagging") != null) {
                    action = S3BucketTask.ACTION.PutBucketTagging;
                } else {
                    action = S3BucketTask.ACTION.CreateBucket;
                }
                break;
            case "POST":
                if (request.getParameter("delete") != null) {
                    action = S3BucketTask.ACTION.DeleteObjects;
                }
                break;
            case "HEAD":
                if (!StringUtils.isEmpty(bucket)) {
                    action = S3BucketTask.ACTION.HeadBucket;
                }
                break;
            case "DELETE":
                if (request.getParameter("delete") != null) {
                    action = S3BucketTask.ACTION.DeleteBucketTagging;
                } else {
                    action = S3BucketTask.ACTION.DeleteBucket;
                }
                break;
            default:
                break;
        }
        return task;
        return task;
    }

    public Response handleTask() {
        return Response.ok().build();
    }

    public Response doListObjects() {
        return Response.ok().build();
    }

}
