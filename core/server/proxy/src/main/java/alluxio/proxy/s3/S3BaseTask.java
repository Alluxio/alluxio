package alluxio.proxy.s3;

import javax.ws.rs.core.Response;

public abstract class S3BaseTask {

    public enum OpType {
        // Object Task
        ListParts,
        GetObjectTagging,
        PutObjectTagging,
        DeleteObjectTagging,
        GetObject,
        PutObject,
        CopyObject,
        DeleteObject,
        HeadObject,
        UploadPart,
        UploadPartCopy,
        CreateMultipartUpload,
        AbortMultipartUpload,
        // Bucket Task
        ListBuckets,
        GetBucketTagging,
        PutBucketTagging,
        DeleteBucketTagging,
        CreateBucket,
        ListObjects, // as well as ListObjectsV2
        DeleteObjects,
        HeadBucket,
        DeleteBucket,
        Unsupported
    }
    protected S3Handler mHandler;
    protected OpType mOPType;
    public S3BaseTask(S3Handler handler) {
        this(handler, OpType.Unsupported);
    }

    public S3BaseTask(S3Handler handler, OpType opType) {
        mHandler = handler;
        mOPType = opType;
    }

    public abstract Response continueTask();

}
