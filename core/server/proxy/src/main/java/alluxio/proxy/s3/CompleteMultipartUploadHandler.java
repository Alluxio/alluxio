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

package alluxio.proxy.s3;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.io.ByteStreams;
import org.apache.commons.codec.binary.Hex;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Callable;

/**
 * A handler process multipart upload complete response.
 */
public class CompleteMultipartUploadHandler extends AbstractHandler {
  private static final Logger LOG = LoggerFactory.getLogger(CompleteMultipartUploadHandler.class);

  private final String mS3Prefix = Constants.REST_API_PREFIX + AlluxioURI.SEPARATOR
      + S3RestServiceHandler.SERVICE_PREFIX + AlluxioURI.SEPARATOR;

  private final FileSystem mFileSystem;
  private final ExecutorService mExecutor;
  private final Long mKeepAliveTime;

  /**
   * Creates a new instance of {@link CompleteMultipartUploadHandler}.
   * @param fs instance of {@link FileSystem}
   */
  public CompleteMultipartUploadHandler(final FileSystem fs) {
    mFileSystem = fs;
    mExecutor = Executors.newFixedThreadPool(ServerConfiguration.getInt(
        PropertyKey.PROXY_S3_COMPLETE_MULTIPART_UPLOAD_POOL_SIZE));
    mKeepAliveTime = ServerConfiguration.getMs(
        PropertyKey.PROXY_S3_COMPLETE_MULTIPART_UPLOAD_KEEPALIVE_TIME_INTERVAL);
  }

  /**
   * Process s3 multipart upload request.
   */
  @Override
  public void handle(String s, Request request, HttpServletRequest
      httpServletRequest, HttpServletResponse httpServletResponse)
      throws IOException, ServletException {
    if (s.startsWith(mS3Prefix)
        && request.getMethod().equals("POST")
        && request.getParameter("uploadId") != null) {

      final String bucket = s.substring(mS3Prefix.length(), s.lastIndexOf(AlluxioURI.SEPARATOR));
      final String object = s.substring(s.lastIndexOf(AlluxioURI.SEPARATOR) + 1);
      final Long uploadId = Long.valueOf(request.getParameter("uploadId"));
      httpServletResponse.setStatus(HttpServletResponse.SC_OK);
      httpServletResponse.setContentType("text/xml");

      Future<CompleteMultipartUploadResult> future =
          mExecutor.submit(new CompleteMultipartUploadTask(mFileSystem, bucket, object, uploadId));
      while (!future.isDone()) {
        try {
          Thread.sleep(mKeepAliveTime);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        //periodically sends white space characters to keep the connection from timing out
        httpServletResponse.getWriter().print(" ");
        httpServletResponse.getWriter().flush();
      }

      XmlMapper mapper = new XmlMapper();

      try {
        CompleteMultipartUploadResult result = future.get();
        httpServletResponse.getWriter().write(mapper.writeValueAsString(result));
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
      httpServletResponse.getWriter().flush();
      request.setHandled(true);
    }
  }

  /**
   * Complete Multipart upload Task.
   */
  public class CompleteMultipartUploadTask implements
      Callable<CompleteMultipartUploadResult> {

    private final FileSystem mFileSystem;
    private final String mBucket;
    private final String mObject;
    private final long mUploadId;

    /**
     * Creates a new instance of {@link CompleteMultipartUploadTask}.
     *
     * @param fileSystem instance of {@link FileSystem}
     * @param bucket bucket name
     * @param object object name
     * @param uploadId multipart upload Id
     */
    public CompleteMultipartUploadTask(
        FileSystem fileSystem, String bucket, String object, long uploadId) {
      mFileSystem = fileSystem;
      mBucket = bucket;
      mObject = object;
      mUploadId = uploadId;
    }

    @Override
    public CompleteMultipartUploadResult call() throws S3Exception {
      try {
        String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + mBucket);
        S3RestUtils.checkPathIsAlluxioDirectory(mFileSystem, bucketPath);
        String objectPath = bucketPath + AlluxioURI.SEPARATOR + mObject;
        AlluxioURI multipartTemporaryDir =
            new AlluxioURI(S3RestUtils.getMultipartTemporaryDirForObject(bucketPath, mObject));
        S3RestUtils.checkUploadId(mFileSystem, multipartTemporaryDir, mUploadId);

        List<URIStatus> parts = mFileSystem.listStatus(multipartTemporaryDir);
        parts.sort(new S3RestUtils.URIStatusNameComparator());

        CreateFilePOptions options = CreateFilePOptions.newBuilder().setRecursive(true)
            .setWriteType(S3RestUtils.getS3WriteType()).build();
        FileOutStream os = mFileSystem.createFile(new AlluxioURI(objectPath), options);
        MessageDigest md5 = MessageDigest.getInstance("MD5");

        try (DigestOutputStream digestOutputStream = new DigestOutputStream(os, md5)) {
          for (URIStatus part : parts) {
            try (FileInStream is = mFileSystem.openFile(new AlluxioURI(part.getPath()))) {
              ByteStreams.copy(is, digestOutputStream);
            }
          }
        }

        mFileSystem.delete(multipartTemporaryDir,
            DeletePOptions.newBuilder().setRecursive(true).build());
        MultipartUploadCleaner.cancelAbort(mFileSystem, mBucket, mObject, mUploadId);
        String entityTag = Hex.encodeHexString(md5.digest());
        return new CompleteMultipartUploadResult(objectPath, mBucket, mObject, entityTag);
      } catch (Exception e) {
        return new CompleteMultipartUploadResult(S3ErrorCode.Name.INTERNAL_ERROR, e.getMessage());
      }
    }
  }
}
