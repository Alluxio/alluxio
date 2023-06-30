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

package alluxio.s3;

import static io.netty.handler.codec.http.HttpResponseStatus.ACCEPTED;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import alluxio.AlluxioURI;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.security.authentication.AuthType;
import alluxio.util.ThreadUtils;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.ws.rs.core.MediaType;

/**
 * Utilities for handling S3 REST calls.
 */
public class NettyRestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(NettyRestUtils.class);

  /**
   * Calls the given {@link NettyRestUtils.RestCallable} and handles any exceptions thrown.
   *
   * @param <T> the return type of the callable
   * @param resource the resource (bucket or object) to be operated on
   * @param callable the callable to call
   * @return the response object
   */
  public static <T> HttpResponse call(String resource, RestCallable<T> callable) {
    try {
      T result = callable.call();
      HttpVersion version = HttpVersion.HTTP_1_1;
      if (result == null) {
        return new DefaultFullHttpResponse(version, OK);
      } else if (result instanceof HttpResponse) {
        return (HttpResponse) result;
      }
      if (result instanceof HttpResponseStatus) {
        if (OK.equals(result)) {
          return new DefaultFullHttpResponse(version, OK);
        } else if (ACCEPTED.equals(result)) {
          return new DefaultFullHttpResponse(version, ACCEPTED);
        } else if (NO_CONTENT.equals(result)) {
          return new DefaultFullHttpResponse(version, NO_CONTENT);
        } else {
          return S3ErrorResponse.createNettyErrorResponse(new S3Exception(
              "Response status is invalid", resource, S3ErrorCode.INTERNAL_ERROR), resource);
        }
      }
      // Need to explicitly encode the string as XML because Jackson will not do it automatically.
      XmlMapper mapper = new XmlMapper();
      ByteBuf contentBuffer =
          Unpooled.copiedBuffer(mapper.writeValueAsString(result), CharsetUtil.UTF_8);
      DefaultFullHttpResponse resp = new DefaultFullHttpResponse(version, OK);
      resp.content().writeBytes(contentBuffer);
      contentBuffer.release();
      resp.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_XML);
      resp.headers().set(HttpHeaderNames.CONTENT_LENGTH, resp.content().readableBytes());
      return resp;
    } catch (Exception e) {
      String errOutputMsg = e.getMessage();
      if (StringUtils.isEmpty(errOutputMsg)) {
        errOutputMsg = ThreadUtils.formatStackTrace(e);
      }
      LOG.warn("Error invoking REST endpoint for {}:\n{}", resource, errOutputMsg);
      return S3ErrorResponse.createNettyErrorResponse(e, resource);
    }
  }

  /**
   * An interface representing a callable.
   *
   * @param <T> the return type of the callable
   */
  public interface RestCallable<T> {
    /**
     * The REST endpoint implementation.
     *
     * @return the return value from the callable
     */
    T call() throws S3Exception;
  }

  /**
   * Given xAttr, parses and returns the Content-Type header metadata
   * as its corresponding {@link MediaType}, or otherwise defaults
   * to {@link MediaType#APPLICATION_OCTET_STREAM_TYPE}.
   * @param xAttr the Inode's xAttrs
   * @return the {@link MediaType} corresponding to the Content-Type header
   */
  public static MediaType deserializeContentType(Map<String, byte[]> xAttr) {
    MediaType type = MediaType.APPLICATION_OCTET_STREAM_TYPE;
    // Fetch the Content-Type from the Inode xAttr
    if (xAttr == null) {
      return type;
    }
    if (xAttr.containsKey(S3Constants.CONTENT_TYPE_XATTR_KEY)) {
      String contentType = new String(xAttr.get(
          S3Constants.CONTENT_TYPE_XATTR_KEY), S3Constants.HEADER_CHARSET);
      if (!contentType.isEmpty()) {
        type = MediaType.valueOf(contentType);
      }
    }
    return type;
  }

  /**
   * This helper method is used to get the ETag xAttr on an object.
   * @param status The {@link URIStatus} of the object
   * @return the entityTag String, or null if it does not exist
   */
  public static String getEntityTag(URIStatus status) {
    if (status.getXAttr() == null
        || !status.getXAttr().containsKey(S3Constants.ETAG_XATTR_KEY)) {
      return null;
    }
    return new String(status.getXAttr().get(S3Constants.ETAG_XATTR_KEY),
        S3Constants.XATTR_STR_CHARSET);
  }

  /**
   * Format bucket path.
   *
   * @param bucketPath bucket path
   * @return bucket path after format
   */
  public static String parsePath(String bucketPath) {
    // Normalize the bucket by replacing ":" with "/"
    return bucketPath.replace(S3Constants.BUCKET_SEPARATOR, AlluxioURI.SEPARATOR);
  }

  /**
   * Convert an exception to instance of {@link S3Exception}.
   *
   * @param exception Exception thrown when process s3 object rest request
   * @param resource complete bucket path
   * @param auditContext the audit context for exception
   * @return instance of {@link S3Exception}
   */
  public static S3Exception toBucketS3Exception(Exception exception, String resource,
                                                @Nonnull S3AuditContext auditContext) {
    if (exception instanceof AccessControlException) {
      auditContext.setAllowed(false);
    }
    auditContext.setSucceeded(false);
    try {
      throw exception;
    } catch (S3Exception e) {
      e.setResource(resource);
      return e;
    } catch (DirectoryNotEmptyException e) {
      return new S3Exception(e, resource, S3ErrorCode.BUCKET_NOT_EMPTY);
    } catch (FileAlreadyExistsException e) {
      return new S3Exception(e, resource, S3ErrorCode.BUCKET_ALREADY_EXISTS);
    } catch (FileDoesNotExistException e) {
      return new S3Exception(e, resource, S3ErrorCode.NO_SUCH_BUCKET);
    } catch (InvalidPathException e) {
      return new S3Exception(e, resource, S3ErrorCode.INVALID_BUCKET_NAME);
    } catch (AccessControlException e) {
      return new S3Exception(e, resource, S3ErrorCode.ACCESS_DENIED_ERROR);
    } catch (Exception e) {
      return new S3Exception(e, resource, S3ErrorCode.INTERNAL_ERROR);
    }
  }

  /**
   * Convert an exception to instance of {@link S3Exception}.
   *
   * @param exception Exception thrown when process s3 object rest request
   * @param resource object complete path
   * @param auditContext the audit context for exception
   * @return instance of {@link S3Exception}
   */
  public static S3Exception toObjectS3Exception(Exception exception, String resource,
                                                @Nonnull S3AuditContext auditContext) {
    if (exception instanceof AccessControlException) {
      auditContext.setAllowed(false);
    }
    auditContext.setSucceeded(false);
    try {
      throw exception;
    } catch (S3Exception e) {
      e.setResource(resource);
      return e;
    } catch (DirectoryNotEmptyException e) {
      return new S3Exception(e, resource, S3ErrorCode.PRECONDITION_FAILED);
    } catch (FileDoesNotExistException | FileNotFoundException e) {
      if (Pattern.matches(ExceptionMessage.BUCKET_DOES_NOT_EXIST.getMessage(".*"),
          e.getMessage())) {
        return new S3Exception(e, resource, S3ErrorCode.NO_SUCH_BUCKET);
      }
      return new S3Exception(e, resource, S3ErrorCode.NO_SUCH_KEY);
    } catch (AccessControlException e) {
      return new S3Exception(e, resource, S3ErrorCode.ACCESS_DENIED_ERROR);
    } catch (Exception e) {
      return new S3Exception(e, resource, S3ErrorCode.INTERNAL_ERROR);
    }
  }

  /**
   * Given xAttr, parses and deserializes the Tagging metadata
   * into a {@link TaggingData} object. Returns null if no data exists.
   * @param xAttr the Inode's xAttrs
   * @return the deserialized {@link TaggingData} object
   */
  public static TaggingData deserializeTags(Map<String, byte[]> xAttr)
      throws IOException {
    // Fetch the S3 tags from the Inode xAttr
    if (xAttr == null || !xAttr.containsKey(S3Constants.TAGGING_XATTR_KEY)) {
      return null;
    }
    return TaggingData.deserialize(xAttr.get(S3Constants.TAGGING_XATTR_KEY));
  }

  /**
   * Get username from header info from FullHttpRequest.
   *
//   * @param authorization
   * @param request FullHttpRequest
   * @return user name
   * @throws S3Exception
   */
  public static String getUser(FullHttpRequest request)
      throws S3Exception {
    String authorization = request.headers().get("Authorization");
    //TODO(wyy) support AwsSignatureProcessor
//    if (S3RestUtils.isAuthenticationEnabled(Configuration.global())) {
//      return getUserFromSignature(request);
//    }
    try {
      return getUserFromAuthorization(authorization, Configuration.global());
    } catch (RuntimeException e) {
      throw new S3Exception(new S3ErrorCode(S3ErrorCode.INTERNAL_ERROR.getCode(),
          e.getMessage(), S3ErrorCode.INTERNAL_ERROR.getStatus()));
    }
  }

  /**
   * Gets the user from the authorization header string for AWS Signature Version 4.
   * @param authorization the authorization header string
   * @param conf the {@link AlluxioConfiguration} Alluxio conf
   * @return the user
   */
  @VisibleForTesting
  public static String getUserFromAuthorization(String authorization, AlluxioConfiguration conf)
      throws S3Exception {
    if (conf.get(PropertyKey.SECURITY_AUTHENTICATION_TYPE) == AuthType.NOSASL) {
      return null;
    }
    if (authorization == null) {
      throw new S3Exception("The authorization header that you provided is not valid.",
          S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
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
      throw new S3Exception("The authorization header that you provided is not valid.",
          S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }
    String credentials = fields[1];
    String[] creds = credentials.split("=");
    // only support version 4 signature
    if (creds.length < 2 || !StringUtils.equals("Credential", creds[0])
        || !creds[1].contains("/")) {
      throw new S3Exception("The authorization header that you provided is not valid.",
          S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }

    final String user = creds[1].substring(0, creds[1].indexOf("/")).trim();
    if (user.isEmpty()) {
      throw new S3Exception("The authorization header that you provided is not valid.",
          S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }

    return user;
  }
}
