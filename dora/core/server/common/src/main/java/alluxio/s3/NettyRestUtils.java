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
import alluxio.s3.auth.Authenticator;
import alluxio.s3.auth.AwsAuthInfo;
import alluxio.s3.signature.AwsSignatureProcessor;
import alluxio.security.authentication.AuthType;
import alluxio.util.ThreadUtils;
import alluxio.wire.FileInfo;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;

/**
 * Utilities for handling S3 REST calls.
 */
public class NettyRestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(NettyRestUtils.class);
  public static final HttpVersion HTTP_VERSION = HttpVersion.HTTP_1_1;
  private static final boolean ENABLED_AUTHENTICATION =
      Configuration.getBoolean(PropertyKey.S3_REST_AUTHENTICATION_ENABLED);
  public static final Authenticator AUTHENTICATOR =
      Authenticator.Factory.create(Configuration.global());

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
      HttpVersion version = HTTP_VERSION;
      if (result == null) {
        return null;
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
      DefaultFullHttpResponse resp = new DefaultFullHttpResponse(version, OK, contentBuffer);
      resp.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_XML);
      resp.headers().set(HttpHeaderNames.CONTENT_LENGTH, contentBuffer.readableBytes());
      return resp;
    } catch (Exception e) {
      String errOutputMsg = e.getMessage();
      if (StringUtils.isEmpty(errOutputMsg)) {
        errOutputMsg = ThreadUtils.formatStackTrace(e);
      }
      LOG.error("Error invoking REST endpoint for {}: {}", resource, errOutputMsg);
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
  @Nullable
  public static String getEntityTag(FileInfo status) {
    String contenthash = status.getContentHash();
    return StringUtils.isNotEmpty(contenthash) ? contenthash : null;
  }

  /**
   * Format bucket path. Normalize the bucket by replacing ":" with "/".
   *
   * @param bucketPath bucket path
   * @return bucket path after format
   */
  public static String parsePath(String bucketPath) {
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
                                                S3AuditContext auditContext) {
    if (exception instanceof AccessControlException) {
      auditContext.setAllowed(false);
    }
    auditContext.setSucceeded(false);
    if (exception instanceof S3Exception) {
      S3Exception e = (S3Exception) exception;
      e.setResource(resource);
      return e;
    } else if (exception instanceof DirectoryNotEmptyException) {
      return new S3Exception(exception, resource, S3ErrorCode.BUCKET_NOT_EMPTY);
    } else if (exception instanceof FileAlreadyExistsException) {
      return new S3Exception(exception, resource, S3ErrorCode.BUCKET_ALREADY_EXISTS);
    } else if (exception instanceof FileDoesNotExistException) {
      return new S3Exception(exception, resource, S3ErrorCode.NO_SUCH_BUCKET);
    } else if (exception instanceof InvalidPathException) {
      return new S3Exception(exception, resource, S3ErrorCode.INVALID_BUCKET_NAME);
    } else if (exception instanceof AccessControlException) {
      return new S3Exception(exception, resource, S3ErrorCode.ACCESS_DENIED_ERROR);
    } else {
      return new S3Exception(exception, resource, S3ErrorCode.INTERNAL_ERROR);
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
                                                S3AuditContext auditContext) {
    if (exception instanceof AccessControlException) {
      auditContext.setAllowed(false);
    }
    auditContext.setSucceeded(false);
    if (exception instanceof S3Exception) {
      S3Exception e = (S3Exception) exception;
      e.setResource(resource);
      return e;
    } else if (exception instanceof DirectoryNotEmptyException) {
      return new S3Exception(exception, resource, S3ErrorCode.PRECONDITION_FAILED);
    } else if (exception instanceof FileDoesNotExistException
        || exception instanceof FileNotFoundException) {
      if (Pattern.matches(ExceptionMessage.BUCKET_DOES_NOT_EXIST.getMessage(".*"),
          exception.getMessage())) {
        return new S3Exception(exception, resource, S3ErrorCode.NO_SUCH_BUCKET);
      }
      return new S3Exception(exception, resource, S3ErrorCode.NO_SUCH_KEY);
    } else if (exception instanceof AccessControlException) {
      return new S3Exception(exception, resource, S3ErrorCode.ACCESS_DENIED_ERROR);
    } else {
      return new S3Exception(exception, resource, S3ErrorCode.INTERNAL_ERROR);
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
   * @param request FullHttpRequest
   * @return user name
   * @throws S3Exception
   */
  public static String getUser(FullHttpRequest request)
      throws S3Exception {
    String authorization = request.headers().get("Authorization");
    if (ENABLED_AUTHENTICATION) {
      return getUserFromSignature(request);
    }
    try {
      return getUserFromAuthorization(authorization, Configuration.global());
    } catch (RuntimeException e) {
      throw new S3Exception(new S3ErrorCode(S3ErrorCode.INTERNAL_ERROR.getCode(),
          e.getMessage(), S3ErrorCode.INTERNAL_ERROR.getStatus()));
    }
  }

  private static String getUserFromSignature(FullHttpRequest request)
      throws S3Exception {
    AwsSignatureProcessor signatureProcessor = new AwsSignatureProcessor(request);
    AwsAuthInfo authInfo = signatureProcessor.getAuthInfo();
    if (AUTHENTICATOR.isAuthenticated(authInfo)) {
      return authInfo.getAccessID();
    }
    throw new S3Exception(authInfo.toString(), S3ErrorCode.INVALID_IDENTIFIER);
  }

  /**
   * Gets the user from the authorization header string for AWS Signature Version 4.
   * @param authorization the authorization header string
   * @param conf the {@link AlluxioConfiguration} Alluxio conf
   * @return the user
   */
  @VisibleForTesting
  @Nullable
  public static String getUserFromAuthorization(String authorization, AlluxioConfiguration conf)
      throws S3Exception {
    if (conf.get(PropertyKey.SECURITY_AUTHENTICATION_TYPE) == AuthType.NOSASL) {
      return null;
    }
    if (StringUtils.isEmpty(authorization)) {
      LOG.error("The authorization header content is null or empty");
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
      LOG.error("The authorization header {} content is invalid: not contain the credential key",
          authorization);
      throw new S3Exception("The authorization header that you provided is not valid.",
          S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }
    String credentials = fields[1];
    String[] creds = credentials.split("=");
    // only support version 4 signature
    if (creds.length < 2 || !StringUtils.equals("Credential", creds[0])
        || !creds[1].contains("/")) {
      LOG.error(
          "The authorization header {} content is invalid: only version 4 signature is supported",
          authorization);
      throw new S3Exception("The authorization header that you provided is not valid.",
          S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }

    final String user = creds[1].substring(0, creds[1].indexOf("/")).trim();
    if (user.isEmpty()) {
      LOG.error("The authorization header {} content is invalid: empty user", authorization);
      throw new S3Exception("The authorization header that you provided is not valid.",
          S3ErrorCode.AUTHORIZATION_HEADER_MALFORMED);
    }

    return user;
  }

  /**
   * Convert MultivaluedMap to a single value map.
   *
   * @param queryParameters MultivaluedMap
   * @param lowerCase whether to use lower case
   * @return a single value map
   */
  public static Map<String, String> fromMultiValueToSingleValueMap(
      MultivaluedMap<String, String> queryParameters, boolean lowerCase) {
    Map<String, String> result = lowerCase
        ? new TreeMap<>(new Comparator<String>() {
          @Override
          public int compare(String o1, String o2) {
            return o1.compareToIgnoreCase(o2);
          }
        }) : new HashMap<>();
    for (String key : queryParameters.keySet()) {
      result.put(key, queryParameters.getFirst(key));
    }
    return result;
  }

  /**
   * Convert {@link HttpHeaders} to a single value map.
   *
   * @param httpHeaders HttpHeaders
   * @return a single value map
   */
  public static Map<String, String> convertToSingleValueMap(HttpHeaders httpHeaders) {
    Map<String, String> headersMap = new HashMap<>();
    for (Map.Entry<String, String> entry : httpHeaders) {
      String key = entry.getKey();
      String value = entry.getValue();
      headersMap.put(key, value);
    }
    return headersMap;
  }

  /**
   * Convert MultivaluedMap to a single value map.
   *
   * @param queryParameters MultivaluedMap
   * @return a single value map
   */
  public static Map<String, String> fromListValueMapToSingleValueMap(
      Map<String, List<String>> queryParameters) {
    Map<String, String> result = new HashMap<>();
    for (Map.Entry<String, List<String>> entry : queryParameters.entrySet()) {
      result.put(entry.getKey(), entry.getValue().get(0));
    }
    return result;
  }

  /**
   * Get the scheme of a {@link FullHttpRequest}.
   *
   * @param fullHttpRequest FullHttpRequest
   * @return the scheme string
   */
  public static String getScheme(FullHttpRequest fullHttpRequest) {
    HttpHeaders headers = fullHttpRequest.headers();
    String hostHeader = headers.get("Host");

    String scheme = "http"; // default scheme is http
    if (hostHeader != null && hostHeader.startsWith("https://")) {
      scheme = "https";
    }
    return scheme;
  }
}
