package alluxio.proxy.s3;

import javax.ws.rs.core.Response;

public abstract class S3BaseTask {

    public enum OpTag {
        LIGHT,
        HEAVY
    }

    public enum OpType {
        // Object Task
        ListParts(OpTag.LIGHT),
        GetObjectTagging(OpTag.LIGHT),
        PutObjectTagging(OpTag.LIGHT),
        DeleteObjectTagging(OpTag.LIGHT),
        GetObject(OpTag.HEAVY),
        PutObject(OpTag.HEAVY),
        CopyObject(OpTag.HEAVY),
        DeleteObject(OpTag.LIGHT),
        HeadObject(OpTag.LIGHT),
        UploadPart(OpTag.LIGHT),
        UploadPartCopy(OpTag.HEAVY),
        CreateMultipartUpload(OpTag.LIGHT),
        AbortMultipartUpload(OpTag.LIGHT),
        CompleteMultipartUpload(OpTag.HEAVY),
        // Bucket Task
        ListBuckets(OpTag.LIGHT),
        ListMultipartUploads(OpTag.LIGHT),
        GetBucketTagging(OpTag.LIGHT),
        PutBucketTagging(OpTag.LIGHT),
        DeleteBucketTagging(OpTag.LIGHT),
        CreateBucket(OpTag.LIGHT),
        ListObjects(OpTag.LIGHT), // as well as ListObjectsV2
        DeleteObjects(OpTag.LIGHT),
        HeadBucket(OpTag.LIGHT),
        DeleteBucket(OpTag.LIGHT),
        Unsupported(OpTag.LIGHT),
        Unknown(OpTag.LIGHT);

        private final OpTag opTag;
        OpType(OpTag opTag) {
            this.opTag = opTag;
        }

        OpTag getOpTag() {
            return this.opTag;
        }
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

    public OpType getOPType() {
        return mOPType;
    }

    public abstract Response continueTask();

    public void handleTaskAsync() {};

}
