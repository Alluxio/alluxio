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
import alluxio.client.WriteType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.grpc.*;
import alluxio.util.ThreadUtils;
import alluxio.web.ProxyWebServer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.base.Stopwatch;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

/**
 * A handler process multipart upload complete response.
 */
public class CompleteMultipartUploadHandler extends AbstractHandler {
  private static final Logger LOG = LoggerFactory.getLogger(CompleteMultipartUploadHandler.class);

  private final String mS3Prefix;

  private final FileSystem mMetaFs;
  private final ExecutorService mExecutor;
  private final boolean mKeepAliveEnabled;
  private final Long mKeepAliveTime;

  /**
   * Creates a new instance of {@link CompleteMultipartUploadHandler}.
   * @param fs instance of {@link FileSystem}
   * @param baseUri the Web server's base URI used for building the handler's matching URI
   */
  public CompleteMultipartUploadHandler(final FileSystem fs, final String baseUri) {
    mMetaFs = fs;
    ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
            .setNameFormat("MULTIPART-UPLOAD-%d").build();
    mExecutor = Executors.newFixedThreadPool(Configuration.getInt(
        PropertyKey.PROXY_S3_COMPLETE_MULTIPART_UPLOAD_POOL_SIZE), namedThreadFactory);
    mKeepAliveEnabled = Configuration.getBoolean(
        PropertyKey.PROXY_S3_COMPLETE_MULTIPART_UPLOAD_KEEPALIVE_ENABLED);
    mKeepAliveTime = Configuration.getMs(
        PropertyKey.PROXY_S3_COMPLETE_MULTIPART_UPLOAD_KEEPALIVE_TIME_INTERVAL);
    mS3Prefix = baseUri + AlluxioURI.SEPARATOR + S3RestServiceHandler.SERVICE_PREFIX;
  }

  /**
   * Process s3 multipart upload request.
   */
  @Override
  public void handle(String s, Request request, HttpServletRequest httpServletRequest,
                     HttpServletResponse httpServletResponse) throws IOException {
    Stopwatch stopwatch = null;
    try {
      if (!s.startsWith(mS3Prefix)) {
        return;
      }
      if (!request.getMethod().equals("POST") || request.getParameter("uploadId") == null) {
        return;
      } // Otherwise, handle CompleteMultipartUpload
      stopwatch = Stopwatch.createStarted();
      final String user;
      try {
        // TODO(czhu): support S3RestServiceHandler.getUserFromSignature()
        //             Ideally migrate both to S3RestUtils and make them static
        user = S3RestUtils.getUserFromAuthorization(
            request.getHeader("Authorization"), mMetaFs.getConf());
      } catch (S3Exception e) {
        XmlMapper mapper = new XmlMapper();
        S3Error errorResponse = new S3Error("Authorization", e.getErrorCode());
        httpServletResponse.setStatus(e.getErrorCode().getStatus().getStatusCode());
        httpServletResponse.getOutputStream().print(mapper.writeValueAsString(errorResponse));
        request.setHandled(true); // Prevent other handlers from processing this request
        return;
      }
      s = s.substring(mS3Prefix.length() + 1); // substring the prefix + leading "/" character
      final String bucket = s.substring(0, s.indexOf(AlluxioURI.SEPARATOR));
      final String object = s.substring(s.indexOf(AlluxioURI.SEPARATOR) + 1);
      final String uploadId = request.getParameter("uploadId");
      LOG.debug("(bucket: {}, object: {}, uploadId: {}) queuing task...",
          bucket, object, uploadId);

      // Set headers before getting committed when flushing whitespaces
      httpServletResponse.setContentType(MediaType.APPLICATION_XML);

      CompleteMultipartUploadTask task = new CompleteMultipartUploadTask(mMetaFs,
              S3RestUtils.createFileSystemForUser(user, mMetaFs), bucket, object, uploadId,
              IOUtils.toString(request.getReader()));
      Future<CompleteMultipartUploadResult> future = mExecutor.submit(task);
      if (mKeepAliveEnabled) {
        // Set status before getting committed when flushing whitespaces
        httpServletResponse.setStatus(HttpServletResponse.SC_OK);
        long sleepMs = 1000;
        while (!future.isDone()) {
          LOG.debug("(bucket: {}, object: {}, uploadId: {}) sleeping for {}ms...",
              bucket, object, uploadId, sleepMs);
          try {
            Thread.sleep(sleepMs);
          } catch (InterruptedException e) {
            LOG.error(e.toString());
          }
          // TODO(czhu): figure out how to send whitespace characters while still
          // returning a correct status code
          // - calling getWriter().flush() commits the response (headers, status code, etc.)
          // - https://docs.oracle.com/javaee/7/api/javax/servlet/ServletResponse.html#getWriter--
          // periodically sends white space characters to keep the connection from timing out
          LOG.debug("(bucket: {}, object: {}, uploadId: {}) sending whitespace...",
              bucket, object, uploadId);
          httpServletResponse.getWriter().print(" ");
          httpServletResponse.getWriter().flush();
          sleepMs = Math.min(2 * sleepMs, mKeepAliveTime);
        }
      } // otherwise we perform a blocking call on future.get()

      XmlMapper mapper = new XmlMapper();
      try {
        CompleteMultipartUploadResult result = future.get();
        httpServletResponse.getWriter().write(mapper.writeValueAsString(result));
        if (!mKeepAliveEnabled) {
          httpServletResponse.setStatus(HttpServletResponse.SC_OK);
        }
      } catch (Exception e) {
        Throwable cause = e.getCause();
        if (cause instanceof S3Exception) {
          S3Exception s3Exception = (S3Exception) cause;
          httpServletResponse.getWriter().write(mapper.writeValueAsString(
              new CompleteMultipartUploadResult(s3Exception.getErrorCode().getCode(),
                  s3Exception.getErrorCode().getDescription())));
          if (!mKeepAliveEnabled) {
            httpServletResponse.setStatus(s3Exception.getErrorCode().getStatus().getStatusCode());
          }
        }
        LOG.error(ThreadUtils.formatStackTrace(cause));
      }
      httpServletResponse.getWriter().flush();
      request.setHandled(true);
    } catch (Exception e) {
      // This try-catch is not intended to handle any exceptions, it is purely
      // to ensure that encountered exceptions get logged.
      LOG.error("Unhandled exception for {}. {}", s, ThreadUtils.formatStackTrace(e));
      throw e;
    } finally {
      if (stopwatch != null) {
        ProxyWebServer.logAccess(httpServletRequest, httpServletResponse, stopwatch);
      }
    }
  }

  /**
   * Complete Multipart upload Task.
   */
  public class CompleteMultipartUploadTask implements
      Callable<CompleteMultipartUploadResult> {

    private final FileSystem mMetaFs;
    private final FileSystem mUserFs;
    private final String mBucket;
    private final String mObject;
    private final String mUploadId;
    private final String mBody;
    private final boolean mMultipartCleanerEnabled = Configuration.getBoolean(
        PropertyKey.PROXY_S3_MULTIPART_UPLOAD_CLEANER_ENABLED);
    private AlluxioURI mMultipartTemporaryDir;

    /**
     * Creates a new instance of {@link CompleteMultipartUploadTask}.
     *
     * @param metaFs instance of {@link FileSystem} - used for metadata operations
     * @param userFs instance of {@link FileSystem} - under the scope of a user agent
     * @param bucket bucket name
     * @param object object name
     * @param uploadId multipart upload Id
     * @param body the HTTP request body
     */
    public CompleteMultipartUploadTask(FileSystem metaFs, FileSystem userFs,
                                       String bucket, String object,
                                       String uploadId, String body) {
      mMetaFs = metaFs;
      mUserFs = userFs;
      mBucket = bucket;
      mObject = object;
      mUploadId = uploadId;
      mBody = body;
    }

    @Override
    public CompleteMultipartUploadResult call() throws S3Exception {
      String objectPath = null;
      String objTempPath = null;
      try {
        String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + mBucket);
        S3RestUtils.checkPathIsAlluxioDirectory(mUserFs, bucketPath, null);
        objectPath = bucketPath + AlluxioURI.SEPARATOR + mObject;
        // Check for existing multipart info files and dirs
        AlluxioURI multipartTemporaryDir = new AlluxioURI(
            S3RestUtils.getMultipartTemporaryDirForObject(bucketPath, mObject, mUploadId));
        URIStatus metaStatus;
        try {
          metaStatus = S3RestUtils.checkStatusesForUploadId(mMetaFs, mUserFs,
                  multipartTemporaryDir, mUploadId).get(1);
        } catch (Exception e) {
          LOG.warn("checkStatusesForUploadId uploadId:{} failed", mUploadId, ThreadUtils.formatStackTrace(e));
          throw new S3Exception(objectPath, S3ErrorCode.NO_SUCH_UPLOAD);
        }
        // Parse the HTTP request body to get the intended list of parts
        CompleteMultipartUploadRequest request = parseCompleteMultipartUploadRequest(objectPath);

        // Check if the requested parts are available
        List<URIStatus> uploadedParts = validateParts(request, objectPath, multipartTemporaryDir);

        // Only overwrite at final step to commit/complete the file (AKA rename)
//        try {
//          S3RestUtils.deleteExistObject(mUserFs, objectUri);
//        } catch (IOException | AlluxioException e) {
//          throw S3RestUtils.toObjectS3Exception(e, objectUri.getPath());
//        }
        // (re)create the merged object to a temporary object path
        LOG.debug("CompleteMultipartUploadTask (bucket: {}, object: {}, uploadId: {}) "
            + "combining {} parts...", mBucket, mObject, mUploadId, uploadedParts.size());
        CreateFilePOptions createFileOption = prepareForCreateTempFile(metaStatus);
        objTempPath = objectPath + ".temp." + UUID.randomUUID();
        AlluxioURI objectTempUri = new AlluxioURI(objTempPath);
        FileOutStream os = mUserFs.createFile(objectTempUri, createFileOption);
        MessageDigest md5 = MessageDigest.getInstance("MD5");

        try (DigestOutputStream digestOutputStream = new DigestOutputStream(os, md5)) {
          for (URIStatus part : uploadedParts) {
            try (FileInStream is = mUserFs.openFile(new AlluxioURI(part.getPath()))) {
              ByteStreams.copy(is, digestOutputStream);
            }
          }
        }
        // persist the ETag via xAttr
        String entityTag = Hex.encodeHexString(md5.digest());
        // TODO(czhu): try to compute the ETag prior to creating the file to reduce total RPC RTT
        S3RestUtils.setEntityTag(mUserFs, objectTempUri, entityTag);
        // rename the temp file to the target object file path
        AlluxioURI objectUri = new AlluxioURI(objectPath);
        mUserFs.rename(objectTempUri, objectUri, RenamePOptions.newBuilder()
                .setPersist(WriteType.fromProto(createFileOption.getWriteType()).isThrough())
                .setS3SyntaxOptions(S3SyntaxOptions.newBuilder().setOverwrite(true).build())
                .build());

        // Remove the temporary directory containing the uploaded parts and the
        // corresponding Alluxio S3 API metadata file
        removePartsDirAndMPMetaFile(multipartTemporaryDir);
        return new CompleteMultipartUploadResult(objectPath, mBucket, mObject, entityTag);
      } catch (Exception e) {
        /* On exception we always check if someone completes the multipart object before us to
        achieve idempotency: when a race caused by retry(most cases), the commit of
        this object happens at time of rename op, check DefaultFileSystemMaster.rename for more info.
         * */
        LOG.warn("[LUCYINFO] Exception during CompleteMultipartUpload:{}", ThreadUtils.formatStackTrace(e));
        if (objectPath != null) {
          URIStatus objStatus = checkIfComplete(objectPath);
          if (objStatus != null) {
            String etag = new String(objStatus.getXAttr()
                    .getOrDefault(S3Constants.ETAG_XATTR_KEY, new byte[0]));
            if (!etag.isEmpty()) {
              LOG.info("Check for idempotency, uploadId:{} idempotency check passed.", mUploadId);
              return new CompleteMultipartUploadResult(objectPath, mBucket, mObject, etag);
            }
            LOG.info("Check for idempotency, uploadId:{} object path exists but no etag found.", mUploadId);
          }
        }
        throw S3RestUtils.toObjectS3Exception(e, mObject);
      } finally {
        // Cleanup temp obj path no matter what, if path not exist, ignore
        cleanupTempPath(objTempPath);
      }
    }

    public CreateFilePOptions prepareForCreateTempFile(URIStatus metaStatus) {
      CreateFilePOptions.Builder optionsBuilder = CreateFilePOptions.newBuilder()
              .setRecursive(true)
              .setMode(PMode.newBuilder()
                      .setOwnerBits(Bits.ALL)
                      .setGroupBits(Bits.ALL)
                      .setOtherBits(Bits.NONE).build())
              .putXattr(PropertyKey.Name.S3_UPLOADS_ID_XATTR_KEY, ByteString.copyFrom(mUploadId, StandardCharsets.UTF_8))
              .setXattrPropStrat(XAttrPropagationStrategy.LEAF_NODE)
              .setWriteType(S3RestUtils.getS3WriteType());
      // Copy Tagging xAttr if it exists
      if (metaStatus.getXAttr().containsKey(S3Constants.TAGGING_XATTR_KEY)) {
        optionsBuilder.putXattr(S3Constants.TAGGING_XATTR_KEY,
                ByteString.copyFrom(metaStatus.getXAttr().get(S3Constants.TAGGING_XATTR_KEY)));
      }
      // Copy Content-Type Header xAttr if it exists
      if (metaStatus.getXAttr().containsKey(S3Constants.CONTENT_TYPE_XATTR_KEY)) {
        optionsBuilder.putXattr(S3Constants.CONTENT_TYPE_XATTR_KEY,
                ByteString.copyFrom(metaStatus.getXAttr().get(S3Constants.CONTENT_TYPE_XATTR_KEY)));
      }
      return optionsBuilder.build();
    }

    public CompleteMultipartUploadRequest parseCompleteMultipartUploadRequest(String objectPath)
            throws S3Exception {
      CompleteMultipartUploadRequest request;
      try {
        request = new XmlMapper().readerFor(CompleteMultipartUploadRequest.class)
                .readValue(mBody);
      } catch (IllegalArgumentException | JsonProcessingException e) {
        LOG.error("Failed parsing CompleteMultipartUploadRequest:{}",
                ThreadUtils.formatStackTrace(e));
        Throwable cause = e.getCause();
        if (cause instanceof S3Exception) {
          throw S3RestUtils.toObjectS3Exception((S3Exception) cause, objectPath);
        }
        throw S3RestUtils.toObjectS3Exception(e, objectPath);
      }
      return request;
    }

    public List<URIStatus> validateParts(CompleteMultipartUploadRequest request,
                                         String objectPath,
                                         AlluxioURI multipartTemporaryDir)
            throws S3Exception, IOException, AlluxioException {
      List<URIStatus> uploadedParts = mUserFs.listStatus(multipartTemporaryDir);
      uploadedParts.sort(new S3RestUtils.URIStatusNameComparator());
      if (uploadedParts.size() < request.getParts().size()) {
        throw new S3Exception(objectPath, S3ErrorCode.INVALID_PART);
      }
      Map<Integer, URIStatus> uploadedPartsMap = uploadedParts.stream().collect(Collectors.toMap(
              status -> Integer.parseInt(status.getName()),
              status -> status
      ));
      int lastPartNum = request.getParts().get(request.getParts().size() - 1).getPartNumber();
      for (CompleteMultipartUploadRequest.Part part : request.getParts()) {
        if (!uploadedPartsMap.containsKey(part.getPartNumber())) {
          throw new S3Exception(objectPath, S3ErrorCode.INVALID_PART);
        }
        if (part.getPartNumber() != lastPartNum // size requirement not applicable to last part
                && uploadedPartsMap.get(part.getPartNumber()).getLength() < Configuration.getBytes(
                PropertyKey.PROXY_S3_COMPLETE_MULTIPART_UPLOAD_MIN_PART_SIZE)) {
          throw new S3Exception(objectPath, S3ErrorCode.ENTITY_TOO_SMALL);
        }
      }
      return uploadedParts;
    }

    public void removePartsDirAndMPMetaFile(AlluxioURI multipartTemporaryDir) throws IOException, AlluxioException {
      mUserFs.delete(multipartTemporaryDir,
              DeletePOptions.newBuilder().setRecursive(true).build());
      mMetaFs.delete(new AlluxioURI(
                      S3RestUtils.getMultipartMetaFilepathForUploadId(mUploadId)),
              DeletePOptions.newBuilder().build());
      if (mMultipartCleanerEnabled) {
        MultipartUploadCleaner.cancelAbort(mMetaFs, mUserFs, mBucket, mObject, mUploadId);
      }
    }

    public void cleanupTempPath(String objTempPath) {
      if (objTempPath != null) {
        try {
          mUserFs.delete(new AlluxioURI(objTempPath), DeletePOptions.newBuilder().build());
        } catch (Exception e) {
          LOG.warn("Failed to clean up temp path:{}, {}", objTempPath, e.getMessage());
        }
      }
    }

    public URIStatus checkIfComplete(String objectPath) {
      try {
        URIStatus objStatus = mUserFs.getStatus(new AlluxioURI(objectPath));
        String uploadId = new String(objStatus.getXAttr()
                .getOrDefault(PropertyKey.Name.S3_UPLOADS_ID_XATTR_KEY, new byte[0]));
        if (objStatus.isCompleted() && StringUtils.equals(uploadId, mUploadId)) {
          return objStatus;
        }
      } catch (IOException | AlluxioException ex) {
        // can't validate if any previous attempt has succeeded
        return null;
      }
      return null;
    }
  }
}
