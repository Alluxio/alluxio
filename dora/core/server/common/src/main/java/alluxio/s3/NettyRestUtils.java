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
import alluxio.client.file.URIStatus;
import alluxio.exception.AccessControlException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.util.ThreadUtils;
import alluxio.wire.FileInfo;
import javax.annotation.Nonnull;
import javax.ws.rs.core.MediaType;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyRestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(NettyRestUtils.class);

  public static <T> HttpResponse call(String resource, RestCallable<T> callable) {
    try {
      T result = callable.call();
      HttpVersion version = HttpVersion.HTTP_1_0;
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
      FullHttpResponse resp = new DefaultFullHttpResponse(version, OK, contentBuffer);
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
  public static String getEntityTag(FileInfo status) {
    if (status.getXAttr() == null
        || !status.getXAttr().containsKey(S3Constants.ETAG_XATTR_KEY)) {
      return null;
    }
    return new String(status.getXAttr().get(S3Constants.ETAG_XATTR_KEY),
        S3Constants.XATTR_STR_CHARSET);
  }

  /**
   * Convert an exception to instance of {@link S3Exception}.
   *
   * @param exception Exception thrown when process s3 object rest request
   * @param resource object complete path
   * @return instance of {@link S3Exception}
   */
  public static S3Exception toObjectS3Exception(Exception exception, String resource) {
    try {
      throw exception;
    } catch (S3Exception e) {
      e.setResource(resource);
      return e;
    } catch (DirectoryNotEmptyException e) {
      return new S3Exception(e, resource, S3ErrorCode.PRECONDITION_FAILED);
    } catch (FileDoesNotExistException|FileNotFoundException e) {
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
    return toObjectS3Exception(exception, resource);
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
}
