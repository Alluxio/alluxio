/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.s3;

import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpResponse;

/**
 * S3 Abstract Base task for handling S3 API logic in netty server.
 */
public abstract class S3NettyBaseTask {
  protected final S3NettyHandler mHandler;
  protected final OpType mOPType;

  /**
   * Creates an instance of {@link S3NettyBaseTask}.
   * @param handler S3NettyHandler
   * @param OPType op type
   */
  public S3NettyBaseTask(S3NettyHandler handler, OpType OPType) {
    mHandler = handler;
    mOPType = OPType;
  }

  /**
   * Return the OpType (S3 API enum).
   *
   * @return OpType (S3 API enum)
   */
  public OpType getOPType() {
    return mOPType;
  }

  /**
   * Run core S3 API logic from different S3 task.
   *
   * @return HttpResponse (S3 Http response)
   */
  public abstract HttpResponse continueTask();

  /**
   * Run core S3 API logic with HttpContent in different S3 task.
   * @param content HttpContent
   * @return HttpResponse (S3 Http response)
   */
  public HttpResponse handleContent(HttpContent content) {
    return null;
  }

  /**
   * Return if the S3 API needs to process the content.
   * @return if true, the S3 API needs to process the content
   */
  public boolean needContent() {
    return false;
  }

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
