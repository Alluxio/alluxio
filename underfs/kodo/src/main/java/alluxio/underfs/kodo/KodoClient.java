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

package alluxio.underfs.kodo;

import alluxio.exception.status.NotFoundException;

import com.qiniu.common.QiniuException;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.UploadManager;
import com.qiniu.storage.model.FileInfo;
import com.qiniu.storage.model.FileListing;
import com.qiniu.util.Auth;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.http.HttpStatus;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * Client or Kodo under file system.
 */
public class KodoClient {

  /** Qiniu authentication for Qiniu SDK. */
  private Auth mAuth;

  /** Bucket manager for Qiniu SDK. */
  private BucketManager mBucketManager;

  /** Upload manager for Qiniu SDK. */
  private UploadManager mUploadManager;

  /** Http client for get Object. */
  private OkHttpClient mOkHttpClient;

  /** Bucket name of the Alluxio Kodo bucket. */
  private String mBucketName;

  /** Qiniu Kodo download Host. */
  private String mDownloadHost;

  /** Endpoint for Qiniu kodo. */
  private String mEndPoint;
  /**
   * Creates a new instance of {@link KodoClient}.
   * @param auth Qiniu authentication
   * @param bucketName bucketname for kodo
   * @param downloadHost download host for kodo
   * @param endPoint endpoint for kodo
   * @param cfg configuration for Qiniu SDK
   * @param okHttpClient http client
   */
  public KodoClient(Auth auth, String bucketName, String downloadHost, String endPoint,
      Configuration cfg, OkHttpClient okHttpClient) {
    mAuth = auth;
    mBucketName = bucketName;
    mEndPoint = endPoint;
    mDownloadHost = downloadHost;
    mBucketManager = new BucketManager(mAuth, cfg);
    mUploadManager = new UploadManager(cfg);
    mOkHttpClient = okHttpClient;
  }
  /**
   * Gets bucketname for kodoclient.
   * @return bucketname
   */
  public String getBucketName() {
    return mBucketName;
  }

  /**
   * Gets file from for Qiniu kodo.
   * @param key Object jey
   * @return Qiniu FileInfo
   * @throws QiniuException
   */
  public FileInfo getFileInfo(String key) throws QiniuException {
    return mBucketManager.stat(mBucketName, key);
  }
  /**
   * Gets object from Qiniu kodo. All requests are authenticated by default，default expires 3600s We
   * use okhttp as our HTTP client and support two main parameters in the external adjustment, MAX
   * request and timeout time.
   * @param key object key
   * @param startPos start index for object
   * @param endPos end index for object
   * @return inputstream
   * @param contentLength object file size
   * @throws IOException
   */
  public InputStream getObject(String key, long startPos, long endPos, long contentLength)
      throws IOException {
    String baseUrl = String.format("http://%s/%s", mDownloadHost, key);
    String privateUrl = mAuth.privateDownloadUrl(baseUrl);
    URL url = new URL(privateUrl);
    String objectUrl = String.format("http://%s/%s?%s", mEndPoint, key, url.getQuery());
    Request request = new Request.Builder().url(objectUrl)
        .addHeader("Range",
            "bytes=" + String.valueOf(startPos) + "-"
                + String.valueOf(endPos < contentLength ? endPos - 1 : contentLength - 1))
        .addHeader("Host", mDownloadHost).get().build();
    Response response = mOkHttpClient.newCall(request).execute();
    if (response.code() == HttpStatus.SC_NOT_FOUND) {
      throw new NotFoundException("Qiniu kodo: " + response.message());
    }
    if (response.code() != 200 && response.code() != 206) {
      throw new IOException(String.format("Qiniu kodo:get object failed errcode:%d,errmsg:%s",
          response.code(), response.message()));
    }
    return response.body().byteStream();
  }

  /**
   * Puts Object to Qiniu kodo.
   * @param Key Object key for kodo
   * @param File Alluxio File
   */
  public void uploadFile(String Key, File File) throws QiniuException {
    com.qiniu.http.Response response =
        mUploadManager.put(File, Key, mAuth.uploadToken(mBucketName, Key));
    response.close();
  }
  /**
   * Copys object in Qiniu kodo.
   * @param src source Object key
   * @param dst destination Object Key
   */
  public void copyObject(String src, String dst) throws QiniuException {
    mBucketManager.copy(mBucketName, src, mBucketName, dst);
  }
  /**
   * Creates empty Object in Qiniu kodo.
   * @param key empty Object key
   */
  public void createEmptyObject(String key) throws QiniuException {
    com.qiniu.http.Response response =
        mUploadManager.put(new byte[0], key, mAuth.uploadToken(mBucketName, key));
    response.close();
  }
  /**
   * Deletes Object in Qiniu kodo.
   * @param key Object key
   */
  public void deleteObject(String key) throws QiniuException {
    com.qiniu.http.Response response = mBucketManager.delete(mBucketName, key);
    response.close();
  }
  /**
   * Lists object for Qiniu kodo.
   * @param prefix prefix for bucket
   * @param marker Marker returned the last time a file list was obtained
   * @param limit Length limit for each iteration, Max. 1000
   * @param delimiter Specifies a directory separator that lists all common prefixes (simulated
   *        listing directory effects). The default is an empty string
   * @return result for list
   */
  public FileListing listFiles(String prefix, String marker, int limit, String delimiter)
      throws QiniuException {
    return mBucketManager.listFiles(mBucketName, prefix, marker, limit, delimiter);
  }
}
