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

package alluxio.underfs.response;

/**
 * The single multipart upload task info.
 */
public class MultipartUploadInfo {
  String mKey;
  String mUploadId;
  String mInitiated;

  /**
   * Constructs a default {@link MultipartUploadInfo}.
   * @param key the path of the file
   * @param uploadId the upload id of the multipart upload task
   * @param initiated the initiated date
   */
  public MultipartUploadInfo(String key, String uploadId, String initiated) {
    mKey = key;
    mUploadId = uploadId;
    mInitiated = initiated;
  }

  /**
   * @return the object key
   */
  public String getKey() {
    return mKey;
  }

  /**
   * @return the timestamp for when the multipart upload was initiated
   */
  public String getInitiated() {
    return mInitiated;
  }

  /**
   * @return the upload ID
   */
  public String getUploadId() {
    return mUploadId;
  }

  /**
   * @param key the S3 object key
   */
  public void setKey(String key) {
    mKey = key;
  }

  /**
   * @param initiated the creation timestamp of the multipart upload
   */
  public void setInitiated(String initiated) {
    mInitiated = initiated;
  }

  /**
   * @param uploadId the upload ID string
   */
  public void setUploadId(String uploadId) {
    mUploadId = uploadId;
  }
}
