package alluxio.proxy.s3;

import javax.ws.rs.core.Response;

/**
 * S3 Abstract Base task for handling S3 API logic.
 */
public abstract class S3BaseTask {

  protected S3Handler mHandler;
  protected OpType mOPType;

  /**
   * Instantiate a S3BaseTask.
   * @param handler S3Handler object
   */
  public S3BaseTask(S3Handler handler) {
    this(handler, OpType.Unsupported);
  }

  /**
   * Instantiate a S3BaseTask.
   * @param handler S3Handler object
   * @param opType  the enum indicate the S3 API name
   */
  public S3BaseTask(S3Handler handler, OpType opType) {
    mHandler = handler;
    mOPType = opType;
  }

  /**
   * Return the OpType (S3 API enum).
   * @return OpType (S3 API enum)
   */
  public OpType getOPType() {
    return mOPType;
  }

  /**
   * Run core S3 API logic from different S3 task.
   * @return Response object containing common HTTP response properties
   */
  public abstract Response continueTask();

  /**
   * Run S3 API logic in a customized async way, e.g. delegate the
   * core API logic to another thread and do something while waiting.
   */
  public void handleTaskAsync() {
  }

  /**
   * Enum for tagging the http request to target for
   * different threadpools for handling.
   */
  public enum OpTag {
    LIGHT, HEAVY
  }

  /**
   * Enum indicating name of S3 API handling per http request.
   */
  public enum OpType {

    // Object Task
    ListParts(OpTag.LIGHT),
    GetObjectTagging(OpTag.LIGHT),
    PutObjectTagging(OpTag.LIGHT),
    DeleteObjectTagging(OpTag.LIGHT),
    GetObject(OpTag.HEAVY), PutObject(OpTag.HEAVY),
    CopyObject(OpTag.HEAVY), DeleteObject(OpTag.LIGHT),
    HeadObject(OpTag.LIGHT), UploadPart(OpTag.LIGHT),
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

    private final OpTag mOpTag;

    OpType(OpTag opTag) {
      mOpTag = opTag;
    }

    OpTag getOpTag() {
      return mOpTag;
    }
  }
}
