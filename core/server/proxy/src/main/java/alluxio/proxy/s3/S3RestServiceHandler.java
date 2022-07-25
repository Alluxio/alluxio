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
import alluxio.conf.PropertyKey;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.Configuration;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.proxy.s3.auth.AwsAuthInfo;
import alluxio.proxy.s3.auth.Authenticator;
import alluxio.proxy.s3.signature.AwsSignatureProcessor;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.XAttrPropagationStrategy;
import alluxio.proto.journal.File;
import alluxio.security.User;
import alluxio.web.ProxyWebServer;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.common.net.InetAddresses;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;
import javax.security.auth.Subject;
import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * This class is a REST handler for Amazon S3 API.
 */
@NotThreadSafe
@Path(S3RestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_XML)
@Consumes({ MediaType.TEXT_XML, MediaType.APPLICATION_XML,
    MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_FORM_URLENCODED })
public final class S3RestServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(S3RestServiceHandler.class);

  public static final String SERVICE_PREFIX = "s3";

  /* Bucket is the first component in the URL path. */
  public static final String BUCKET_PARAM = "{bucket}/";
  /* Object is after bucket in the URL path */
  public static final String OBJECT_PARAM = "{bucket}/{object:.+}";

  private final FileSystem mFileSystem;
  private final InstancedConfiguration mSConf;

  @Context
  private ContainerRequestContext mRequestContext;

  private final boolean mBucketNamingRestrictionsEnabled;
  private final int mMaxHeaderMetadataSize; // 0 means disabled
  private final boolean mMultipartCleanerEnabled;

  private final Pattern mBucketAdjacentDotsDashesPattern;
  private final Pattern mBucketInvalidPrefixPattern;
  private final Pattern mBucketInvalidSuffixPattern;
  private final Pattern mBucketValidNamePattern;

  /**
   * Constructs a new {@link S3RestServiceHandler}.
   *
   * @param context context for the servlet
   */
  public S3RestServiceHandler(@Context ServletContext context) {
    mFileSystem =
        (FileSystem) context.getAttribute(ProxyWebServer.FILE_SYSTEM_SERVLET_RESOURCE_KEY);
    mSConf = (InstancedConfiguration)
        context.getAttribute(ProxyWebServer.SERVER_CONFIGURATION_RESOURCE_KEY);

    mBucketNamingRestrictionsEnabled = Configuration.getBoolean(
        PropertyKey.PROXY_S3_BUCKET_NAMING_RESTRICTIONS_ENABLED);
    mMaxHeaderMetadataSize = (int) Configuration.getBytes(
        PropertyKey.PROXY_S3_METADATA_HEADER_MAX_SIZE);
    mMultipartCleanerEnabled = Configuration.getBoolean(
        PropertyKey.PROXY_S3_MULTIPART_UPLOAD_CLEANER_ENABLED);

    // https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
    // - Undocumented edge-case, no adjacent periods with hyphens, i.e: '.-' or '-.'
    mBucketAdjacentDotsDashesPattern = Pattern.compile("([-\\.]{2})");
    mBucketInvalidPrefixPattern = Pattern.compile("^xn--.*");
    mBucketInvalidSuffixPattern = Pattern.compile(".*-s3alias$");
    mBucketValidNamePattern = Pattern.compile("[a-z0-9][a-z0-9\\.-]{1,61}[a-z0-9]");
  }

  /**
   * Get username from header info.
   *
   * @param authorization authorization info
   * @return username
   * @throws S3Exception
   */
  private String getUser(String authorization) throws S3Exception {
    if (S3RestUtils.isAuthenticationEnabled(mSConf)) {
      return getUserFromSignature();
    } else {
      return getUserFromAuthorization(authorization);
    }
  }

  /**
   * Get username from parsed header info.
   *
   * @return username
   * @throws S3Exception
   */
  private String getUserFromSignature() throws S3Exception {
    AwsSignatureProcessor signatureProcessor = new AwsSignatureProcessor(mRequestContext);
    Authenticator authenticator = Authenticator.Factory.create(mSConf);
    AwsAuthInfo authInfo = signatureProcessor.getAuthInfo();
    if (authenticator.isAuthenticated(authInfo)) {
      return authInfo.getAccessID();
    }
    throw new S3Exception(authInfo.toString(), S3ErrorCode.INVALID_IDENTIFIER);
  }

  /**
   * Gets the user from the authorization header string for AWS Signature Version 4.
   * @param authorization the authorization header string
   * @return the user
   */
  @VisibleForTesting
  public static String getUserFromAuthorization(String authorization) {
    if (authorization == null) {
      return null;
    }

    // Parse the authorization header defined at
    // https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-auth-using-authorization-header.html
    // All other authorization types are deprecated or EOL (as of writing)
    // Example Header value (spaces turned to line breaks):
    // AWS4-HMAC-SHA256
    // Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,
    // SignedHeaders=host;range;x-amz-date,
    // Signature=fe5f80f77d5fa3beca038a248ff027d0445342fe2855ddc963176630326f1024

    // We only care about the credential key, so split the header by " " and then take everything
    // after the "=" and before the first "/"
    String[] fields = authorization.split(" ");
    if (fields.length < 2) {
      return null;
    }
    String credentials = fields[1];
    String[] creds = credentials.split("=");
    if (creds.length < 2) {
      return null;
    }

    final String user = creds[1].substring(0, creds[1].indexOf("/")).trim();
    if (user.isEmpty()) {
      return null;
    }

    return user;
  }

  private FileSystem getFileSystem(String authorization) throws S3Exception {
    final String user = getUser(authorization);

    if (user == null) {
      return mFileSystem;
    }

    final Subject subject = new Subject();
    subject.getPrincipals().add(new User(user));
    return FileSystem.Factory.get(subject, mSConf);
  }

  /**
   * Lists all buckets owned by you.
   *
   * @param authorization header parameter authorization
   * @return the response object
   */
  @GET
  public Response listAllMyBuckets(@HeaderParam("Authorization") final String authorization) {
    return S3RestUtils.call("", () -> {
      String user = getUserFromAuthorization(authorization);

      List<URIStatus> objects;
      try {
        objects = getFileSystem(authorization).listStatus(new AlluxioURI("/"));
      } catch (AlluxioException | IOException e) {
        throw new RuntimeException(e);
      }

      final List<URIStatus> buckets = objects.stream()
          .filter((uri) -> uri.getOwner().equals(user))
          // debatable (?) potentially breaks backcompat(?)
          .filter(URIStatus::isFolder)
          .collect(Collectors.toList());
      return new ListAllMyBucketsResult(buckets);
    });
  }

  /**
   * Gets a bucket and lists all the objects or bucket tags in it.
   * @param authorization header parameter authorization
   * @param bucket the bucket name
   * @param markerParam the optional marker param
   * @param prefixParam the optional prefix param
   * @param delimiterParam the optional delimiter param
   * @param encodingTypeParam optional encoding type param
   * @param maxKeysParam the optional max keys param
   * @param listTypeParam if listObjectV2 request
   * @param continuationTokenParam the optional continuationToken param for listObjectV2
   * @param startAfterParam  the optional startAfter param for listObjectV2
   * @param tagging query string to indicate if this is for GetBucketTagging
   * @param acl query string to indicate if this is for GetBucketAcl
   * @param policy query string to indicate if this is for GetBucketPolicy
   * @param policyStatus query string to indicate if this is for GetBucketPolicyStatus
   * @param uploads query string to indicate if this is for ListMultipartUploads
   * @return the response object
   */
  @GET
  @Path(BUCKET_PARAM)
  public Response getBucket(@HeaderParam("Authorization") final String authorization,
                            @PathParam("bucket") final String bucket,
                            @QueryParam("marker") final String markerParam,
                            @QueryParam("prefix") final String prefixParam,
                            @QueryParam("delimiter") final String delimiterParam,
                            @QueryParam("encoding-type") final String encodingTypeParam,
                            @QueryParam("max-keys") final Integer maxKeysParam,
                            @QueryParam("list-type") final Integer listTypeParam,
                            @QueryParam("continuation-token") final String continuationTokenParam,
                            @QueryParam("start-after") final String startAfterParam,
                            @QueryParam("tagging") final String tagging,
                            @QueryParam("acl") final String acl,
                            @QueryParam("policy") final String policy,
                            @QueryParam("policyStatus") final String policyStatus,
                            @QueryParam("uploads") final String uploads) {
    return S3RestUtils.call(bucket, () -> {
      Preconditions.checkNotNull(bucket, "required 'bucket' parameter is missing");
      if (acl != null) {
        throw new S3Exception(bucket, new S3ErrorCode(
            S3ErrorCode.INTERNAL_ERROR.getCode(),
            "GetBucketAcl is not currently supported.",
            S3ErrorCode.INTERNAL_ERROR.getStatus()));
      }
      if (policy != null) {
        throw new S3Exception(bucket, new S3ErrorCode(
            S3ErrorCode.INTERNAL_ERROR.getCode(),
            "GetBucketPolicy is not currently supported.",
            S3ErrorCode.INTERNAL_ERROR.getStatus()));
      }
      if (policyStatus != null) {
        throw new S3Exception(bucket, new S3ErrorCode(
            S3ErrorCode.INTERNAL_ERROR.getCode(),
            "GetBucketpolicyStatus is not currently supported.",
            S3ErrorCode.INTERNAL_ERROR.getStatus()));
      }

      String path = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + bucket);
      final FileSystem fs = getFileSystem(authorization);
      S3RestUtils.checkPathIsAlluxioDirectory(fs, path);

      if (tagging != null) { // GetBucketTagging
        AlluxioURI uri = new AlluxioURI(path);
        try {
          TaggingData tagData = S3RestUtils.deserializeTags(fs.getStatus(uri).getXAttr());
          LOG.debug("GetBucketTagging tagData={}", tagData);
          return tagData != null ? tagData : new TaggingData();
        } catch (Exception e) {
          throw S3RestUtils.toBucketS3Exception(e, bucket);
        }
      }
      if (uploads != null) { // ListMultipartUploads
        try {
          List<URIStatus> children = fs.listStatus(new AlluxioURI(
              S3RestUtils.MULTIPART_UPLOADS_METADATA_DIR));
          return ListMultipartUploadsResult.buildFromStatuses(bucket, children);
        } catch (Exception e) {
          throw S3RestUtils.toBucketS3Exception(e, bucket);
        }
      }
      // Otherwise, this is ListObjects(v2)
      int maxKeys = maxKeysParam == null ? ListBucketOptions.DEFAULT_MAX_KEYS : maxKeysParam;
      ListBucketOptions listBucketOptions = ListBucketOptions.defaults()
          .setMarker(markerParam)
          .setPrefix(prefixParam)
          .setMaxKeys(maxKeys)
          .setDelimiter(delimiterParam)
          .setEncodingType(encodingTypeParam)
          .setListType(listTypeParam)
          .setContinuationToken(continuationTokenParam)
          .setStartAfter(startAfterParam);

      List<URIStatus> children;
      try {
        // TODO(czhu): allow non-"/" delimiters by parsing the prefix & delimiter pair to determine
        //             what directory to list the contents of
        // only list the direct children if delimiter is not null
        if (delimiterParam != null) {
          if (prefixParam == null) {
            path = parsePathWithDelimiter(path, "", delimiterParam);
          } else {
            path = parsePathWithDelimiter(path, prefixParam, delimiterParam);
          }
          children = fs.listStatus(new AlluxioURI(path));
        } else {
          ListStatusPOptions options = ListStatusPOptions.newBuilder().setRecursive(true).build();
          children = fs.listStatus(new AlluxioURI(path), options);
        }
      } catch (FileDoesNotExistException e) {
        // return the proper error code if the bucket doesn't exist. Previously a 500 error was
        // returned which does not match the S3 response behavior
        throw new S3Exception(e, bucket, S3ErrorCode.NO_SUCH_BUCKET);
      } catch (IOException | AlluxioException e) {
        throw new RuntimeException(e);
      }
      return new ListBucketResult(
          bucket,
          children,
          listBucketOptions);
    });
  }

  /**
   * Currently implements the DeleteObjects request type if the query parameter "delete" exists.
   *
   * @param authorization header parameter authorization
   * @param bucket the bucket name
   * @param delete the delete query parameter. Existence indicates to run the DeleteObjects impl
   * @param contentLength body content length
   * @param is the input stream to read the request
   *
   * @return a {@link DeleteObjectsResult} if this was a DeleteObjects request
   */
  @POST
  @Path(BUCKET_PARAM)
  public Response postBucket(@HeaderParam("Authorization") final String authorization,
                             @PathParam("bucket") final String bucket,
                             @QueryParam("delete") String delete,
                             @HeaderParam("Content-Length") int contentLength,
                             final InputStream is) {
    return S3RestUtils.call(bucket, () -> {
      // TODO(czhu): Support "Authorization" header

      String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + bucket);
      if (delete != null) { // DeleteObjects
        try {
          final FileSystem fs = getFileSystem(authorization);
          DeleteObjectsRequest request = new XmlMapper().readerFor(DeleteObjectsRequest.class)
              .readValue(is);
          List<DeleteObjectsRequest.DeleteObject> objs =
               request.getToDelete();
          List<DeleteObjectsResult.DeletedObject> success = new ArrayList<>();
          List<DeleteObjectsResult.ErrorObject> errored = new ArrayList<>();
          objs.sort(Comparator.comparingInt(x -> -1 * x.getKey().length()));
          objs.forEach(obj -> {
            try {
              AlluxioURI uri = new AlluxioURI(bucketPath
                  + AlluxioURI.SEPARATOR + obj.getKey());
              DeletePOptions options = DeletePOptions.newBuilder().build();
              fs.delete(uri, options);
              DeleteObjectsResult.DeletedObject del = new DeleteObjectsResult.DeletedObject();
              del.setKey(obj.getKey());
              success.add(del);
            } catch (FileDoesNotExistException | DirectoryNotEmptyException e) {
              /*
              FDNE - delete on FDNE should be counted as a success, as there's nothing to do
              DNE - s3 has no concept dirs - if it _is_ a dir, nothing to delete.
               */
              DeleteObjectsResult.DeletedObject del = new DeleteObjectsResult.DeletedObject();
              del.setKey(obj.getKey());
              success.add(del);
            } catch (IOException | AlluxioException e) {
              DeleteObjectsResult.ErrorObject err = new DeleteObjectsResult.ErrorObject();
              err.setKey(obj.getKey());
              err.setMessage(e.getMessage());
              errored.add(err);
            }
          });

          DeleteObjectsResult result = new DeleteObjectsResult();
          if (!request.getQuiet()) {
            result.setDeleted(success);
          }
          result.setErrored(errored);
          return result;
        } catch (IOException e) {
          LOG.debug("Failed to parse DeleteObjects request:", e);
          return Response.Status.BAD_REQUEST;
        }
      } else { // Silently swallow this POST request
        try {
          LOG.debug("Silently swallowing POST request for bucket: {},"
              + "content: {}", bucket, is.read());
        } catch (Exception e) {
          throw S3RestUtils.toBucketS3Exception(e, bucket);
        }
        return Response.Status.OK;
      }
    });
  }

  /**
   * Creates a bucket, or puts bucket tags on an existing bucket.
   * @param authorization header parameter authorization
   * @param bucket the bucket name
   * @param tagging query string to indicate if this is for PutBucketTagging or not
   * @param acl query string to indicate if this is for PutBucketAcl
   * @param policy query string to indicate if this is for PutBucketPolicy
   * @param is the request body
   * @return the response object
   */
  @PUT
  @Path(BUCKET_PARAM)
  @Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_OCTET_STREAM})
  public Response createBucket(@HeaderParam("Authorization") final String authorization,
                               @PathParam("bucket") final String bucket,
                               @QueryParam("tagging") final String tagging,
                               @QueryParam("acl") final String acl,
                               @QueryParam("policy") final String policy,
                               final InputStream is) {
    return S3RestUtils.call(bucket, () -> {
      Preconditions.checkNotNull(bucket, "required 'bucket' parameter is missing");
      if (acl != null) {
        throw new S3Exception(bucket, new S3ErrorCode(
            S3ErrorCode.INTERNAL_ERROR.getCode(),
            "PutBucketAcl is not currently supported.",
            S3ErrorCode.INTERNAL_ERROR.getStatus()));
      }
      if (policy != null) {
        throw new S3Exception(bucket, new S3ErrorCode(
            S3ErrorCode.INTERNAL_ERROR.getCode(),
            "PutBucketPolicy is not currently supported.",
            S3ErrorCode.INTERNAL_ERROR.getStatus()));
      }
      final FileSystem fs = getFileSystem(authorization);
      String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + bucket);

      if (tagging != null) { // PutBucketTagging
        S3RestUtils.checkPathIsAlluxioDirectory(fs, bucketPath);
        try {
          TaggingData tagData = new XmlMapper().readerFor(TaggingData.class)
              .readValue(is);
          LOG.debug("PutBucketTagging tagData={}", tagData);
          Map<String, ByteString> xattrMap = new HashMap<>();
          xattrMap.put(S3Constants.TAGGING_XATTR_KEY, TaggingData.serialize(tagData));
          SetAttributePOptions attrPOptions = SetAttributePOptions.newBuilder()
              .putAllXattr(xattrMap).setXattrUpdateStrategy(File.XAttrUpdateStrategy.UNION_REPLACE)
              .build();
          fs.setAttribute(new AlluxioURI(bucketPath), attrPOptions);
        } catch (IOException e) {
          if (e.getCause() instanceof S3Exception) {
            throw S3RestUtils.toBucketS3Exception((S3Exception) e.getCause(), bucketPath);
          }
          throw new S3Exception(e, bucketPath, S3ErrorCode.MALFORMED_XML);
        } catch (Exception e) {
          throw S3RestUtils.toBucketS3Exception(e, bucketPath);
        }
        return Response.Status.OK;
      }
      // CreateBucket
      if (mBucketNamingRestrictionsEnabled) {
        Matcher m = mBucketAdjacentDotsDashesPattern.matcher(bucket);
        while (m.find()) {
          if (!m.group().equals("--")) {
            throw new S3Exception(bucket, S3ErrorCode.INVALID_BUCKET_NAME);
          }
        }
        if (!mBucketValidNamePattern.matcher(bucket).matches()
            || mBucketInvalidPrefixPattern.matcher(bucket).matches()
            || mBucketInvalidSuffixPattern.matcher(bucket).matches()
            || InetAddresses.isInetAddress(bucket)) {
          throw new S3Exception(bucket, S3ErrorCode.INVALID_BUCKET_NAME);
        }
      }

      // Silently swallow CreateBucket calls on existing buckets
      // - S3 clients may prepend PutObject requests with CreateBucket calls instead of
      //   calling HeadBucket to ensure that the bucket exists
      try {
        URIStatus status = fs.getStatus(new AlluxioURI(bucketPath));
        if (status.isFolder()) { return Response.Status.OK; }
        throw new InvalidPathException("A file already exists at bucket path " + bucketPath);
      } catch (FileDoesNotExistException e) {
        // do nothing, we will create the directory below
      } catch (Exception e) {
        throw S3RestUtils.toBucketS3Exception(e, bucketPath);
      }

      CreateDirectoryPOptions options =
          CreateDirectoryPOptions.newBuilder().setWriteType(S3RestUtils.getS3WriteType()).build();
      try {
        fs.createDirectory(new AlluxioURI(bucketPath), options);
      } catch (Exception e) {
        throw S3RestUtils.toBucketS3Exception(e, bucketPath);
      }
      return Response.Status.OK;
    });
  }

  /**
   * Deletes a bucket, or deletes all tags from an existing bucket.
   * @param authorization header parameter authorization
   * @param bucket the bucket name
   * @param tagging query string to indicate if this is for DeleteBucketTagging or not
   * @param policy query string to indicate if this is for DeleteBucketPolicy or not
   * @return the response object
   */
  @DELETE
  @Path(BUCKET_PARAM)
  public Response deleteBucket(@HeaderParam("Authorization") final String authorization,
                               @PathParam("bucket") final String bucket,
                               @QueryParam("tagging") final String tagging,
                               @QueryParam("policy") final String policy) {
    return S3RestUtils.call(bucket, () -> {
      Preconditions.checkNotNull(bucket, "required 'bucket' parameter is missing");
      if (policy != null) {
        throw new S3Exception(bucket, new S3ErrorCode(
            S3ErrorCode.INTERNAL_ERROR.getCode(),
            "DeleteBucketPolicy is not currently supported.",
            S3ErrorCode.INTERNAL_ERROR.getStatus()));
      }
      final FileSystem fs = getFileSystem(authorization);
      String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + bucket);
      S3RestUtils.checkPathIsAlluxioDirectory(fs, bucketPath);

      if (tagging != null) { // DeleteBucketTagging
        LOG.debug("DeleteBucketTagging bucket={}", bucketPath);
        Map<String, ByteString> xattrMap = new HashMap<>();
        xattrMap.put(S3Constants.TAGGING_XATTR_KEY, ByteString.copyFrom(new byte[0]));
        SetAttributePOptions attrPOptions = SetAttributePOptions.newBuilder()
            .putAllXattr(xattrMap).setXattrUpdateStrategy(File.XAttrUpdateStrategy.DELETE_KEYS)
            .build();
        try {
          fs.setAttribute(new AlluxioURI(bucketPath), attrPOptions);
        } catch (Exception e) {
          throw S3RestUtils.toBucketS3Exception(e, bucketPath);
        }
        return Response.Status.NO_CONTENT;
      }

      // Delete the bucket.
      DeletePOptions options = DeletePOptions.newBuilder().setAlluxioOnly(Configuration
          .get(PropertyKey.PROXY_S3_DELETE_TYPE).equals(Constants.S3_DELETE_IN_ALLUXIO_ONLY))
          .build();
      try {
        fs.delete(new AlluxioURI(bucketPath), options);
      } catch (Exception e) {
        throw S3RestUtils.toBucketS3Exception(e, bucketPath);
      }
      return Response.Status.NO_CONTENT;
    });
  }

  /**
   * Uploads an object or part of an object in multipart upload.
   * @param authorization header parameter authorization
   * @param contentMD5 the optional Base64 encoded 128-bit MD5 digest of the object
   * @param copySourceParam the URL-encoded source path to copy the new file from
   * @param decodedLength the length of the content when in aws-chunked encoding
   * @param contentLength the total length of the request body
   * @param contentTypeParam the content type of the request body
   * @param bucket the bucket name
   * @param object the object name
   * @param partNumber the identification of the part of the object in multipart upload,
   *                   otherwise null
   * @param uploadId the upload ID of the multipart upload, otherwise null
   * @param metadataDirective one of COPY or REPLACE used for CopyObject
   * @param tagging query string to indicate if this is for PutObjectTagging or not
   * @param taggingDirective one of COPY or REPLACE used for CopyObject
   * @param taggingHeader the URL-encoded user tags passed in the header
   * @param acl query string to indicate if this is for PutObjectAcl
   * @param is the request body
   * @return the response object
   */
  @PUT
  @Path(OBJECT_PARAM)
  @Consumes(MediaType.WILDCARD)
  public Response createObjectOrUploadPart(@HeaderParam("Authorization") final String authorization,
                                           @HeaderParam("Content-MD5") final String contentMD5,
                                           @HeaderParam(S3Constants.S3_COPY_SOURCE_HEADER)
                                                 final String copySourceParam,
                                           @HeaderParam("x-amz-decoded-content-length")
                                                 final String decodedLength,
                                           @HeaderParam(S3Constants.S3_METADATA_DIRECTIVE_HEADER)
                                             final S3Constants.Directive metadataDirective,
                                           @HeaderParam(S3Constants.S3_TAGGING_HEADER)
                                             final String taggingHeader,
                                           @HeaderParam(S3Constants.S3_TAGGING_DIRECTIVE_HEADER)
                                             final S3Constants.Directive taggingDirective,
                                           @HeaderParam(S3Constants.S3_CONTENT_TYPE_HEADER)
                                             final String contentTypeParam,
                                           @HeaderParam("Content-Length")
                                             final String contentLength,
                                           @PathParam("bucket") final String bucket,
                                           @PathParam("object") final String object,
                                           @QueryParam("partNumber") final Integer partNumber,
                                           @QueryParam("uploadId") final String uploadId,
                                           @QueryParam("tagging") final String tagging,
                                           @QueryParam("acl") final String acl,
                                           final InputStream is) {
    return S3RestUtils.call(bucket, () -> {
      Preconditions.checkNotNull(bucket, "required 'bucket' parameter is missing");
      Preconditions.checkNotNull(object, "required 'object' parameter is missing");
      if (acl != null) {
        throw new S3Exception(object, new S3ErrorCode(
            S3ErrorCode.INTERNAL_ERROR.getCode(),
            "PutObjectAcl is not currently supported.",
            S3ErrorCode.INTERNAL_ERROR.getStatus()));
      }
      Preconditions.checkArgument((partNumber == null && uploadId == null)
          || (partNumber != null && uploadId != null),
          "'partNumber' and 'uploadId' parameter should appear together or be "
          + "missing together.");
      Preconditions.checkArgument(!(partNumber != null && tagging != null),
          "Only one of 'partNumber' and 'tagging' can be set.");
      Preconditions.checkArgument(!(taggingHeader != null && tagging != null),
          String.format("Only one of '%s' and 'tagging' can be set.",
              S3Constants.S3_TAGGING_HEADER));
      Preconditions.checkArgument(!(copySourceParam != null && tagging != null),
          String.format("Only one of '%s' and 'tagging' can be set.",
              S3Constants.S3_COPY_SOURCE_HEADER));
      // Uncomment the following check when supporting ACLs
      // Preconditions.checkArgument(!(copySourceParam != null && acl != null),
      //     String.format("Must use the header \"%s\" to provide ACL for CopyObject.",
      //         S3Constants.S3_ACL_HEADER));

      final FileSystem fs = getFileSystem(authorization);
      String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + bucket);
      S3RestUtils.checkPathIsAlluxioDirectory(fs, bucketPath);
      String objectPath = bucketPath + AlluxioURI.SEPARATOR + object;

      CreateDirectoryPOptions dirOptions = CreateDirectoryPOptions.newBuilder()
          .setRecursive(true)
          .setAllowExists(true)
          .build();

      if (objectPath.endsWith(AlluxioURI.SEPARATOR)) {
        // Need to create a folder
        // TODO(czhu): verify S3 behaviour when ending an object path with a delimiter
        // - this is a convenience method for the Alluxio fs which does not have a
        //   direct counterpart for S3, since S3 does not have "folders" as actual objects
        try {
          fs.createDirectory(new AlluxioURI(objectPath), dirOptions);
        } catch (FileAlreadyExistsException e) {
          // ok if directory already exists the user wanted to create it anyway
          LOG.warn("attempting to create dir which already exists");
        } catch (IOException | AlluxioException e) {
          throw S3RestUtils.toObjectS3Exception(e, objectPath);
        }
        return Response.ok().build();
      }

      if (partNumber != null) {
        // This object is part of a multipart upload, should be uploaded into the temporary
        // directory first.
        String tmpDir = S3RestUtils.getMultipartTemporaryDirForObject(bucketPath, object, uploadId);
        try {
          S3RestUtils.checkStatusesForUploadId(fs, new AlluxioURI(tmpDir), uploadId);
        } catch (Exception e) {
          throw S3RestUtils.toObjectS3Exception(e, object);
        }
        objectPath = tmpDir + AlluxioURI.SEPARATOR + partNumber;
      }
      AlluxioURI objectUri = new AlluxioURI(objectPath);

      // Parse the TaggingData
      TaggingData tagData = null;
      if (tagging != null) { // PutObjectTagging
        try {
          tagData = new XmlMapper().readerFor(TaggingData.class).readValue(is);
        } catch (IOException e) {
          if (e.getCause() instanceof S3Exception) {
            throw S3RestUtils.toObjectS3Exception((S3Exception) e.getCause(), objectPath);
          }
          throw new S3Exception(e, objectPath, S3ErrorCode.MALFORMED_XML);
        }
      }
      if (taggingHeader != null) { // Parse the tagging header if it exists for PutObject
        try {
          tagData = S3RestUtils.deserializeTaggingHeader(taggingHeader, mMaxHeaderMetadataSize);
        } catch (IllegalArgumentException e) {
          if (e.getCause() instanceof S3Exception) {
            throw S3RestUtils.toObjectS3Exception((S3Exception) e.getCause(), objectPath);
          }
          throw S3RestUtils.toObjectS3Exception(e, objectPath);
        }
      }
      LOG.debug("PutObjectTagging tagData={}", tagData);

      // Populate the xattr Map with the metadata tags if provided
      Map<String, ByteString> xattrMap = new HashMap<>();
      if (tagData != null) {
        try {
          xattrMap.put(S3Constants.TAGGING_XATTR_KEY, TaggingData.serialize(tagData));
        } catch (Exception e) {
          throw S3RestUtils.toObjectS3Exception(e, objectPath);
        }
      }

      if (tagging != null) { // PutObjectTagging
        try {
          SetAttributePOptions attrPOptions = SetAttributePOptions.newBuilder()
              .putAllXattr(xattrMap).setXattrUpdateStrategy(File.XAttrUpdateStrategy.UNION_REPLACE)
              .build();
          fs.setAttribute(objectUri, attrPOptions);
        } catch (Exception e) {
          throw S3RestUtils.toObjectS3Exception(e, objectPath);
        }
        return Response.ok().build();
      } // else this request is for PutObject

      // populate the xAttr map with the "Content-Type" header
      if (contentTypeParam != null) {
        xattrMap.put(S3Constants.CONTENT_TYPE_XATTR_KEY,
            ByteString.copyFrom(contentTypeParam, S3Constants.HEADER_CHARSET));
      }
      CreateFilePOptions filePOptions =
          CreateFilePOptions.newBuilder().setRecursive(true)
              .setWriteType(S3RestUtils.getS3WriteType())
              .putAllXattr(xattrMap).setXattrPropStrat(XAttrPropagationStrategy.LEAF_NODE)
              .build();

      // not copying from an existing file
      if (copySourceParam == null) {
        try {
          MessageDigest md5 = MessageDigest.getInstance("MD5");

          // The request body can be in the aws-chunked encoding format, or not encoded at all
          // determine if it's encoded, and then which parts of the stream to read depending on
          // the encoding type.
          boolean isChunkedEncoding = decodedLength != null;
          long toRead;
          InputStream readStream = is;
          if (isChunkedEncoding) {
            toRead = Long.parseLong(decodedLength);
            readStream = new ChunkedEncodingInputStream(is);
          } else {
            toRead = Long.parseLong(contentLength);
          }
          try {
            S3RestUtils.deleteExistObject(fs, objectUri);
          } catch (IOException | AlluxioException e) {
            throw S3RestUtils.toObjectS3Exception(e, objectUri.getPath());
          }
          FileOutStream os = fs.createFile(objectUri, filePOptions);
          try (DigestOutputStream digestOutputStream = new DigestOutputStream(os, md5)) {
            long read = ByteStreams.copy(ByteStreams.limit(readStream, toRead), digestOutputStream);
            if (read < toRead) {
              throw new IOException(String.format(
                  "Failed to read all required bytes from the stream. Read %d/%d",
                  read, toRead));
            }
          }

          byte[] digest = md5.digest();
          String base64Digest = BaseEncoding.base64().encode(digest);
          if (contentMD5 != null && !contentMD5.equals(base64Digest)) {
            // The object may be corrupted, delete the written object and return an error.
            try {
              fs.delete(objectUri, DeletePOptions.newBuilder().setRecursive(true).build());
            } catch (Exception e2) {
              // intend to continue and return BAD_DIGEST S3Exception.
            }
            throw new S3Exception(objectUri.getPath(), S3ErrorCode.BAD_DIGEST);
          }

          String entityTag = Hex.encodeHexString(digest);
          // persist the ETag via xAttr
          // TODO(czhu): try to compute the ETag prior to creating the file to reduce total RPC RTT
          S3RestUtils.setEntityTag(fs, objectUri, entityTag);
          if (partNumber != null) { // UploadPart
            return Response.ok().header(S3Constants.S3_ETAG_HEADER, entityTag).build();
          }
          // PutObject
          return Response.ok().header(S3Constants.S3_ETAG_HEADER, entityTag).build();
        } catch (Exception e) {
          throw S3RestUtils.toObjectS3Exception(e, objectPath);
        }
      } else { // CopyObject or UploadPartCopy
        String copySource = !copySourceParam.startsWith(AlluxioURI.SEPARATOR)
            ? AlluxioURI.SEPARATOR + copySourceParam : copySourceParam;
        URIStatus status = null;
        CreateFilePOptions.Builder copyFilePOptionsBuilder = CreateFilePOptions.newBuilder();
        // Handle metadata directive
        if (metadataDirective == S3Constants.Directive.REPLACE
            && filePOptions.getXattrMap().containsKey(S3Constants.CONTENT_TYPE_XATTR_KEY)) {
          copyFilePOptionsBuilder.putXattr(S3Constants.CONTENT_TYPE_XATTR_KEY,
              filePOptions.getXattrMap().get(S3Constants.CONTENT_TYPE_XATTR_KEY));
        } else { // defaults to COPY
          try {
            status = fs.getStatus(new AlluxioURI(copySource));
            if (status.getFileInfo().getXAttr() != null) {
              copyFilePOptionsBuilder.putXattr(S3Constants.CONTENT_TYPE_XATTR_KEY,
                  ByteString.copyFrom(status.getFileInfo().getXAttr().getOrDefault(
                      S3Constants.CONTENT_TYPE_XATTR_KEY,
                      MediaType.APPLICATION_OCTET_STREAM.getBytes(S3Constants.HEADER_CHARSET))));
            }
          } catch (Exception e) {
            throw S3RestUtils.toObjectS3Exception(e, objectPath);
          }
        }
        // Handle tagging directive
        if (taggingDirective == S3Constants.Directive.REPLACE
            && filePOptions.getXattrMap().containsKey(S3Constants.TAGGING_XATTR_KEY)) {
          copyFilePOptionsBuilder.putXattr(S3Constants.TAGGING_XATTR_KEY,
              filePOptions.getXattrMap().get(S3Constants.TAGGING_XATTR_KEY));
        } else { // defaults to COPY
          try {
            if (status == null) {
              status = fs.getStatus(new AlluxioURI(copySource));
            }
            if (status.getFileInfo().getXAttr() != null
                && status.getFileInfo().getXAttr().containsKey(S3Constants.TAGGING_XATTR_KEY)) {
              copyFilePOptionsBuilder.putXattr(S3Constants.TAGGING_XATTR_KEY,
                  TaggingData.serialize(S3RestUtils.deserializeTags(status.getXAttr())));
            }
          } catch (Exception e) {
            throw S3RestUtils.toObjectS3Exception(e, objectPath);
          }
        }
        if (copySource.equals(objectPath)) {
          // do not need to copy a file to itself, unless we are changing file attributes
          // TODO(czhu): support changing metadata via CopyObject to self, verify for UploadPartCopy
          throw new S3Exception("Copying an object to itself invalid.",
              objectPath, S3ErrorCode.INVALID_REQUEST);
        }
        try {
          S3RestUtils.deleteExistObject(fs, objectUri);
        } catch (IOException | AlluxioException e) {
          throw S3RestUtils.toObjectS3Exception(e, objectUri.getPath());
        }
        try (FileInStream in = fs.openFile(new AlluxioURI(copySource));
             FileOutStream out = fs.createFile(objectUri, copyFilePOptionsBuilder.build())) {
          MessageDigest md5 = MessageDigest.getInstance("MD5");
          try (DigestOutputStream digestOut = new DigestOutputStream(out, md5)) {
            IOUtils.copyLarge(in, digestOut, new byte[8 * Constants.MB]);
            byte[] digest = md5.digest();
            String entityTag = Hex.encodeHexString(digest);
            // persist the ETag via xAttr
            // TODO(czhu): compute the ETag prior to creating the file to reduce total RPC RTT
            S3RestUtils.setEntityTag(fs, objectUri, entityTag);
            if (partNumber != null) { // UploadPartCopy
              return new CopyPartResult(entityTag);
            }
            // CopyObject
            return new CopyObjectResult(entityTag, System.currentTimeMillis());
          } catch (IOException e) {
            try {
              out.cancel();
            } catch (Throwable t2) {
              e.addSuppressed(t2);
            }
            throw e;
          }
        } catch (Exception e) {
          throw S3RestUtils.toObjectS3Exception(e, objectPath);
        }
      }
    });
  }

  /**
   * Initiates or completes a multipart upload based on query parameters.
   * @param authorization header parameter Authorization
   * @param contentType header parameter Content-Type
   * @param bucket the bucket name
   * @param object the object name
   * @param uploads the query parameter specifying that this request is to initiate a multipart
   *                upload instead of uploading an object through HTTP multipart forms
   * @param taggingHeader the URL-encoded metadata tags passed in the header
   * @return the response object
   */
  @POST
  @Path(OBJECT_PARAM)
  @Consumes(MediaType.WILDCARD)
  public Response initiateMultipartUpload(
      @HeaderParam("Authorization") final String authorization,
      @HeaderParam(S3Constants.S3_CONTENT_TYPE_HEADER) final String contentType,
      @PathParam("bucket") final String bucket,
      @PathParam("object") final String object,
      @QueryParam("uploads") final String uploads,
      @HeaderParam(S3Constants.S3_TAGGING_HEADER) final String taggingHeader) {
    Preconditions.checkArgument(uploads != null, "parameter 'uploads' should exist");
    return S3RestUtils.call(bucket, () -> {
      FileSystem fs = getFileSystem(authorization);
      String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + bucket);
      S3RestUtils.checkPathIsAlluxioDirectory(fs, bucketPath);
      String objectPath = bucketPath + AlluxioURI.SEPARATOR + object;

      // Populate the xattr Map with the metadata tags if provided
      Map<String, ByteString> xattrMap = new HashMap<>();

      TaggingData tagData = null;
      if (taggingHeader != null) { // Parse the tagging header if it exists for PutObject
        try {
          tagData = S3RestUtils.deserializeTaggingHeader(taggingHeader, mMaxHeaderMetadataSize);
          xattrMap.put(S3Constants.TAGGING_XATTR_KEY, TaggingData.serialize(tagData));
        } catch (S3Exception e) {
          throw e; // rethrow
        } catch (IllegalArgumentException e) {
          if (e.getCause() instanceof S3Exception) {
            throw S3RestUtils.toObjectS3Exception((S3Exception) e.getCause(), objectPath);
          }
          throw S3RestUtils.toObjectS3Exception(e, objectPath);
        } catch (Exception e) {
          throw S3RestUtils.toObjectS3Exception(e, objectPath);
        }
        LOG.debug("InitiateMultipartUpload tagData={}", tagData);
      }

      CreateDirectoryPOptions options = CreateDirectoryPOptions.newBuilder()
          .setRecursive(true).setWriteType(S3RestUtils.getS3WriteType()).build();
      try {
        // Find an unused UUID
        String uploadId;
        do {
          uploadId = UUID.randomUUID().toString();
        } while (fs.exists(
            new AlluxioURI(S3RestUtils.getMultipartMetaFilepathForUploadId(uploadId))));

        // Create the directory containing the upload parts
        AlluxioURI multipartTemporaryDir = new AlluxioURI(
            S3RestUtils.getMultipartTemporaryDirForObject(bucketPath, object, uploadId));
        fs.createDirectory(multipartTemporaryDir, options);

        // Create the Alluxio multipart upload metadata file
        if (contentType != null) {
          xattrMap.put(S3Constants.CONTENT_TYPE_XATTR_KEY,
              ByteString.copyFrom(contentType, S3Constants.HEADER_CHARSET));
        }
        xattrMap.put(S3Constants.UPLOADS_BUCKET_XATTR_KEY,
            ByteString.copyFrom(bucket, S3Constants.XATTR_STR_CHARSET));
        xattrMap.put(S3Constants.UPLOADS_OBJECT_XATTR_KEY,
            ByteString.copyFrom(object, S3Constants.XATTR_STR_CHARSET));
        xattrMap.put(S3Constants.UPLOADS_FILE_ID_XATTR_KEY, ByteString.copyFrom(
                Longs.toByteArray(fs.getStatus(multipartTemporaryDir).getFileId())));
        fs.createFile(
            new AlluxioURI(S3RestUtils.getMultipartMetaFilepathForUploadId(uploadId)),
            CreateFilePOptions.newBuilder()
                .setRecursive(true)
                .setWriteType(S3RestUtils.getS3WriteType())
                .putAllXattr(xattrMap).setXattrPropStrat(XAttrPropagationStrategy.LEAF_NODE)
                .build()
        );
        if (mMultipartCleanerEnabled) {
          MultipartUploadCleaner.apply(fs, bucket, object, uploadId);
        }
        return new InitiateMultipartUploadResult(bucket, object, uploadId);
      } catch (Exception e) {
        throw S3RestUtils.toObjectS3Exception(e, objectPath);
      }
    });
  }

  /**
   * Retrieves an object's metadata.
   * @param authorization header parameter authorization
   * @param bucket the bucket name
   * @param object the object name
   * @return the response object
   */
  @HEAD
  @Path(OBJECT_PARAM)
  public Response getObjectMetadata(@HeaderParam("Authorization") final String authorization,
                                    @PathParam("bucket") final String bucket,
                                    @PathParam("object") final String object) {
    return S3RestUtils.call(bucket, () -> {
      Preconditions.checkNotNull(bucket, "required 'bucket' parameter is missing");
      Preconditions.checkNotNull(object, "required 'object' parameter is missing");

      final FileSystem fs = getFileSystem(authorization);
      String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + bucket);
      S3RestUtils.checkPathIsAlluxioDirectory(fs, bucketPath);
      String objectPath = bucketPath + AlluxioURI.SEPARATOR + object;
      AlluxioURI objectUri = new AlluxioURI(objectPath);

      try {
        URIStatus status = fs.getStatus(objectUri);
        if (status.isFolder() && !object.endsWith(AlluxioURI.SEPARATOR)) {
          throw new FileDoesNotExistException(status.getPath() + " is a directory");
        }
        Response.ResponseBuilder res = Response.ok()
            .lastModified(new Date(status.getLastModificationTimeMs()))
            .header(S3Constants.S3_CONTENT_LENGTH_HEADER,
                status.isFolder() ? 0 : status.getLength());

        // Check for the object's ETag
        String entityTag = S3RestUtils.getEntityTag(status);
        if (entityTag != null) {
          res.header(S3Constants.S3_ETAG_HEADER, entityTag);
        } else {
          LOG.debug("Failed to find ETag for object: " + objectPath);
        }

        // Check if the object had a specified "Content-Type"
        res.type(S3RestUtils.deserializeContentType(status.getXAttr()));
        return res.build();
      } catch (FileDoesNotExistException e) {
        // must be null entity (content length 0) for S3A Filesystem
        return Response.status(404).entity(null).header("Content-Length", "0").build();
      } catch (Exception e) {
        throw S3RestUtils.toObjectS3Exception(e, objectPath);
      }
    });
  }

  /**
   * Downloads an object or list parts of the object in multipart upload.
   * @param authorization header parameter authorization
   * @param bucket the bucket name
   * @param object the object name
   * @param uploadId the ID of the multipart upload, if not null, listing parts of the object
   * @param range the http range header
   * @param tagging query string to indicate if this is for GetObjectTagging or not
   * @param acl query string to indicate if this is for GetObjectAcl or not
   * @return the response object
   */
  @GET
  @Path(OBJECT_PARAM)
  @Produces({MediaType.APPLICATION_OCTET_STREAM,
      MediaType.APPLICATION_XML, MediaType.WILDCARD})
  public Response getObjectOrListParts(@HeaderParam("Authorization") final String authorization,
                                       @HeaderParam("Range") final String range,
                                       @PathParam("bucket") final String bucket,
                                       @PathParam("object") final String object,
                                       @QueryParam("uploadId") final String uploadId,
                                       @QueryParam("tagging") final String tagging,
                                       @QueryParam("acl") final String acl) {
    Preconditions.checkNotNull(bucket, "required 'bucket' parameter is missing");
    Preconditions.checkNotNull(object, "required 'object' parameter is missing");
    Preconditions.checkArgument(!(uploadId != null && tagging != null),
        "Only one of 'uploadId' or 'tagging' can be set");

    if (uploadId != null) {
      return listParts(authorization, bucket, object, uploadId);
    } else if (tagging != null) {
      return getObjectTags(authorization, bucket, object);
    } if (acl != null) {
      return S3RestUtils.call(bucket, () -> {
        throw new S3Exception(object, new S3ErrorCode(
            S3ErrorCode.INTERNAL_ERROR.getCode(),
            "GetObjectAcl is not currently supported.",
            S3ErrorCode.INTERNAL_ERROR.getStatus()
        ));
      });
    } else {
      return getObject(authorization, bucket, object, range);
    }
  }

  // TODO(cc): support paging during listing parts, currently, all parts are returned at once.
  private Response listParts(final String authorization,
                             final String bucket,
                             final String object,
                             final String uploadId) {
    return S3RestUtils.call(bucket, () -> {
      FileSystem fs = getFileSystem(authorization);
      String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + bucket);
      S3RestUtils.checkPathIsAlluxioDirectory(fs, bucketPath);

      AlluxioURI tmpDir = new AlluxioURI(
          S3RestUtils.getMultipartTemporaryDirForObject(bucketPath, object, uploadId));
      try {
        S3RestUtils.checkStatusesForUploadId(fs, tmpDir, uploadId);
      } catch (Exception e) {
        throw S3RestUtils.toObjectS3Exception(e, object);
      }

      try {
        List<URIStatus> statuses = fs.listStatus(tmpDir);
        statuses.sort(new S3RestUtils.URIStatusNameComparator());

        List<ListPartsResult.Part> parts = new ArrayList<>();
        for (URIStatus status : statuses) {
          parts.add(ListPartsResult.Part.fromURIStatus(status));
        }

        ListPartsResult result = new ListPartsResult();
        result.setBucket(bucketPath);
        result.setKey(object);
        result.setUploadId(uploadId);
        result.setParts(parts);
        return result;
      } catch (Exception e) {
        throw S3RestUtils.toObjectS3Exception(e, tmpDir.getPath());
      }
    });
  }

  private Response getObject(final String authorization,
                             final String bucket,
                             final String object,
                             final String range) {
    return S3RestUtils.call(bucket, () -> {
      FileSystem fs = getFileSystem(authorization);
      String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + bucket);
      S3RestUtils.checkPathIsAlluxioDirectory(fs, bucketPath);
      String objectPath = bucketPath + AlluxioURI.SEPARATOR + object;
      AlluxioURI objectUri = new AlluxioURI(objectPath);

      try {
        URIStatus status = fs.getStatus(objectUri);
        FileInStream is = fs.openFile(objectUri);
        S3RangeSpec s3Range = S3RangeSpec.Factory.create(range);
        RangeFileInStream ris = RangeFileInStream.Factory.create(is, status.getLength(), s3Range);

        Response.ResponseBuilder res = Response.ok(ris)
            .lastModified(new Date(status.getLastModificationTimeMs()))
            .header(S3Constants.S3_CONTENT_LENGTH_HEADER, s3Range.getLength(status.getLength()));

        // Check for the object's ETag
        String entityTag = S3RestUtils.getEntityTag(status);
        if (entityTag != null) {
          res.header(S3Constants.S3_ETAG_HEADER, entityTag);
        } else {
          LOG.debug("Failed to find ETag for object: " + objectPath);
        }

        // Check if the object had a specified "Content-Type"
        res.type(S3RestUtils.deserializeContentType(status.getXAttr()));

        // Check if object had tags, if so we need to return the count
        // in the header "x-amz-tagging-count"
        TaggingData tagData = S3RestUtils.deserializeTags(status.getXAttr());
        if (tagData != null) {
          int taggingCount = tagData.getTagMap().size();
          if (taggingCount > 0) {
            res.header(S3Constants.S3_TAGGING_COUNT_HEADER, taggingCount);
          }
        }
        return res.build();
      } catch (Exception e) {
        throw S3RestUtils.toObjectS3Exception(e, objectPath);
      }
    });
  }

  // Helper for GetObjectTagging
  private Response getObjectTags(final String authorization, final String bucket,
                                 final String object) {
    return S3RestUtils.call(bucket, () -> {
      FileSystem fs = getFileSystem(authorization);
      String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + bucket);
      S3RestUtils.checkPathIsAlluxioDirectory(fs, bucketPath);
      String objectPath = bucketPath + AlluxioURI.SEPARATOR + object;
      AlluxioURI uri = new AlluxioURI(objectPath);
      try {
        TaggingData tagData = S3RestUtils.deserializeTags(fs.getStatus(uri).getXAttr());
        LOG.debug("GetObjectTagging tagData={}", tagData);
        return tagData != null ? tagData : new TaggingData();
      } catch (Exception e) {
        throw S3RestUtils.toObjectS3Exception(e, objectPath);
      }
    });
  }

  /**
   * Deletes an object, an object's tags, or aborts a multipart upload.
   * @param authorization header parameter authorization
   * @param bucket the bucket name
   * @param object the object name
   * @param tagging query string to indicate if this is for DeleteObjectTagging or not
   * @param uploadId the upload ID which identifies the incomplete multipart upload to be aborted
   * @return the response object
   */
  @DELETE
  @Path(OBJECT_PARAM)
  public Response deleteObjectOrAbortMultipartUpload(
      @HeaderParam("Authorization") final String authorization,
      @PathParam("bucket") final String bucket,
      @PathParam("object") final String object,
      @QueryParam("uploadId") final String uploadId,
      @QueryParam("tagging") final String tagging) {
    return S3RestUtils.call(bucket, () -> {
      Preconditions.checkNotNull(bucket, "required 'bucket' parameter is missing");
      Preconditions.checkNotNull(object, "required 'object' parameter is missing");
      Preconditions.checkArgument(!(uploadId != null && tagging != null),
          "Only one of uploadId or tagging can be set");

      final FileSystem fs = getFileSystem(authorization);

      if (uploadId != null) {
        abortMultipartUpload(fs, bucket, object, uploadId);
      } else if (tagging != null) {
        deleteObjectTags(fs, bucket, object);
      } else {
        deleteObject(fs, bucket, object);
      }

      // Note: the normal response for S3 delete key is 204 NO_CONTENT, not 200 OK
      return Response.Status.NO_CONTENT;
    });
  }

  private void abortMultipartUpload(FileSystem fs, String bucket, String object, String uploadId)
      throws S3Exception {
    String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + bucket);
    S3RestUtils.checkPathIsAlluxioDirectory(fs, bucketPath);
    String objectPath = bucketPath + AlluxioURI.SEPARATOR + object;
    AlluxioURI multipartTemporaryDir =
        new AlluxioURI(S3RestUtils.getMultipartTemporaryDirForObject(bucketPath, object, uploadId));
    try {
      S3RestUtils.checkStatusesForUploadId(fs, multipartTemporaryDir, uploadId);
    } catch (Exception e) {
      throw S3RestUtils.toObjectS3Exception(e, object);
    }

    try {
      fs.delete(multipartTemporaryDir,
          DeletePOptions.newBuilder().setRecursive(true).build());
      fs.delete(new AlluxioURI(
              S3RestUtils.getMultipartMetaFilepathForUploadId(uploadId)),
          DeletePOptions.newBuilder().build());
      if (mMultipartCleanerEnabled) {
        MultipartUploadCleaner.cancelAbort(fs, bucket, object, uploadId);
      }
    } catch (Exception e) {
      throw S3RestUtils.toObjectS3Exception(e, objectPath);
    }
  }

  private void deleteObject(FileSystem fs, String bucket, String object) throws S3Exception {
    String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + bucket);
    S3RestUtils.checkPathIsAlluxioDirectory(fs, bucketPath);
    // Delete the object.
    String objectPath = bucketPath + AlluxioURI.SEPARATOR + object;
    DeletePOptions options = DeletePOptions.newBuilder().setAlluxioOnly(Configuration
        .get(PropertyKey.PROXY_S3_DELETE_TYPE).equals(Constants.S3_DELETE_IN_ALLUXIO_ONLY))
        .build();
    try {
      fs.delete(new AlluxioURI(objectPath), options);
    } catch (FileDoesNotExistException | DirectoryNotEmptyException e) {
      // intentionally do nothing, this is ok. It should result in a 204 error
      // This is the same response behavior as AWS's S3.
    } catch (Exception e) {
      throw S3RestUtils.toObjectS3Exception(e, objectPath);
    }
  }

  private void deleteObjectTags(FileSystem fs, String bucket, String object)
      throws S3Exception {
    String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + bucket);
    S3RestUtils.checkPathIsAlluxioDirectory(fs, bucketPath);
    String objectPath = bucketPath + AlluxioURI.SEPARATOR + object;
    LOG.debug("DeleteObjectTagging object={}", object);
    Map<String, ByteString> xattrMap = new HashMap<>();
    xattrMap.put(S3Constants.TAGGING_XATTR_KEY, ByteString.copyFrom(new byte[0]));
    SetAttributePOptions attrPOptions = SetAttributePOptions.newBuilder()
        .putAllXattr(xattrMap).setXattrUpdateStrategy(File.XAttrUpdateStrategy.DELETE_KEYS)
        .build();
    try {
      fs.setAttribute(new AlluxioURI(objectPath), attrPOptions);
    } catch (Exception e) {
      throw S3RestUtils.toObjectS3Exception(e, objectPath);
    }
  }

  private String parsePathWithDelimiter(String bucketPath, String prefix, String delimiter)
      throws S3Exception {
    // TODO(czhu): allow non-"/" delimiters
    // Alluxio only support use / as delimiter
    if (!delimiter.equals(AlluxioURI.SEPARATOR)) {
      throw new S3Exception(bucketPath, new S3ErrorCode(
          S3ErrorCode.PRECONDITION_FAILED.getCode(),
          "Alluxio S3 API only support / as delimiter.",
          S3ErrorCode.PRECONDITION_FAILED.getStatus()));
    }
    char delim = AlluxioURI.SEPARATOR.charAt(0);
    String normalizedBucket =
        bucketPath.replace(S3Constants.BUCKET_SEPARATOR, AlluxioURI.SEPARATOR);
    String normalizedPrefix = normalizeS3Prefix(prefix, delim);

    if (!normalizedPrefix.isEmpty() && !normalizedPrefix.startsWith(AlluxioURI.SEPARATOR)) {
      normalizedPrefix = AlluxioURI.SEPARATOR + normalizedPrefix;
    }
    return normalizedBucket + normalizedPrefix;
  }

  /**
   * Normalize the prefix from S3 request.
   **/
  private String normalizeS3Prefix(String prefix, char delimiter) {
    if (prefix != null) {
      int pos = prefix.lastIndexOf(delimiter);
      if (pos >= 0) {
        return prefix.substring(0, pos + 1);
      }
    }
    return "";
  }
}
