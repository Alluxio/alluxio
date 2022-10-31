package alluxio.proxy.s3;

import org.apache.commons.lang3.StringUtils;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import java.util.function.Supplier;

public class S3BucketTask extends S3BaseTask {

    public S3BucketTask(String bucket, String object,
                        HttpServletRequest request,
                        HttpServletResponse response) {
        super(bucket, object, request, response);
    }

    ACTION action_;

    public enum ACTION {
        ListBuckets,
        GetBucketTagging,
        PutBucketTagging,
        DeleteBucketTagging,
        CreateBucket,
        ListObjects, // as well as ListObjectsV2
        DeleteObjects,
        HeadBucket,
        DeleteBucket,
        NOOP
    }


    public static S3BucketTask allocateTask(String bucket, String object,
                                            HttpServletRequest request,
                                            HttpServletResponse response) {
        S3BucketTask task = new S3BucketTask(bucket, object, request, response);
        ACTION action;
        switch (request.getMethod()) {
            case "GET":
                if (StringUtils.isEmpty(bucket)) {
                    action = ACTION.ListBuckets;
                } else if (request.getParameter("tagging") != null) {
                    action = ACTION.GetBucketTagging;
                } else {
                    action = ACTION.ListObjects;
                }
                break;
            case "PUT":
                if (request.getParameter("tagging") != null) {
                    action = ACTION.PutBucketTagging;
                } else {
                    action = ACTION.CreateBucket;
                }
                break;
            case "POST":
                if (request.getParameter("delete") != null) {
                    action = ACTION.DeleteObjects;
                }
                break;
            case "HEAD":
                if (!StringUtils.isEmpty(bucket)) {
                    action = ACTION.HeadBucket;
                }
                break;
            case "DELETE":
                if (request.getParameter("delete") != null) {
                    action = ACTION.DeleteBucketTagging;
                } else {
                    action = ACTION.DeleteBucket;
                }
                break;
            default:
                break;
        }
        return task;
    }

    public Response handleTask() {
        return Response.ok().build();
    }

    public Response doListBuckets() {
        return Response.ok().build();
    }

}
