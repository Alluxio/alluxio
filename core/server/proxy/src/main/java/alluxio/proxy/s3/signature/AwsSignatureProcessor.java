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

package alluxio.proxy.s3.signature;

import static alluxio.proxy.s3.S3Constants.S3_SIGN_DATE;

import alluxio.proxy.s3.S3ErrorCode;
import alluxio.proxy.s3.S3Exception;
import alluxio.proxy.s3.S3RestUtils;
import alluxio.proxy.s3.auth.AwsAuthInfo;
import alluxio.proxy.s3.signature.utils.AwsAuthV2HeaderParserUtils;
import alluxio.proxy.s3.signature.utils.AwsAuthV4HeaderParserUtils;
import alluxio.proxy.s3.signature.utils.AwsAuthV4QueryParserUtils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;

/**
 * Parser to process AWS V2 & V4 auth request. Creates string to sign and auth
 * header. For more details refer to AWS documentation https://docs.aws
 * .amazon.com/general/latest/gr/sigv4-create-canonical-request.html.
 **/

public class AwsSignatureProcessor {
  private static final Logger LOG =
            LoggerFactory.getLogger(AwsSignatureProcessor.class);
  private static final String AUTHORIZATION = "Authorization";

  private ContainerRequestContext mContext;
  private HttpServletRequest mServletRequest;

  /**
   * Create a new {@link AwsSignatureProcessor}.
   *
   * @param context ContainerRequestContext
   */
  public AwsSignatureProcessor(ContainerRequestContext context) {
    mContext = context;
  }

  /**
   * Create a new {@link AwsSignatureProcessor} with HttpServletRequest
   * as the info marshall source.
   * Used by the new architecture in {@link alluxio.proxy.s3.S3RequestServlet}
   *
   * @param request
   */
  public AwsSignatureProcessor(HttpServletRequest request) {
    mServletRequest = request;
  }

  /**
   * Extract signature info from request.
   * @return SignatureInfo
   * @throws S3Exception
   */
  public SignatureInfo parseSignature() throws S3Exception {
    Map<String, String> queryParameters;
    String authHeader;
    String dateHeader;
    if (mContext != null) {
      Map<String, String> headers = S3RestUtils.fromMultiValueToSingleValueMap(
              mContext.getHeaders(), true);
      authHeader = headers.get(AUTHORIZATION);
      dateHeader = headers.get(S3_SIGN_DATE);
      queryParameters = S3RestUtils.fromMultiValueToSingleValueMap(
              mContext.getUriInfo().getQueryParameters(), false);
    } else {
      authHeader = mServletRequest.getHeader(AUTHORIZATION);
      dateHeader = mServletRequest.getHeader(S3_SIGN_DATE);
      queryParameters = new HashMap<>();
      for (Map.Entry<String, String[]> entry : mServletRequest.getParameterMap().entrySet()) {
        queryParameters.put(entry.getKey(), entry.getValue()[0]);
      }
    }

    SignatureInfo signatureInfo;
    if ((signatureInfo =
        AwsAuthV4HeaderParserUtils.parseSignature(authHeader, dateHeader)) != null
        || (signatureInfo =
        AwsAuthV2HeaderParserUtils.parseSignature(authHeader)) != null
        || (signatureInfo =
        AwsAuthV4QueryParserUtils.parseSignature(queryParameters)) != null) {
      return signatureInfo;
    } else {
      LOG.error("Can not parse signature from header information.");
      throw new S3Exception("Can not parse signature from header information.",
          S3ErrorCode.ACCESS_DENIED_ERROR);
    }
  }

  /**
   * Convert SignatureInfo to AwsAuthInfo.
   * @return AwsAuthInfo
   * @throws S3Exception
   */
  public AwsAuthInfo getAuthInfo() throws S3Exception {
    try {
      SignatureInfo signatureInfo = parseSignature();
      String stringToSign = "";
      if (signatureInfo.getVersion() == SignatureInfo.Version.V4) {
        if (mContext != null) {
          stringToSign =
              StringToSignProducer.createSignatureBase(signatureInfo, mContext);
        } else {
          stringToSign =
              StringToSignProducer.createSignatureBase(signatureInfo, mServletRequest);
        }
      }
      String awsAccessId = signatureInfo.getAwsAccessId();
      // ONLY validate aws access id when needed.
      if (StringUtils.isEmpty(awsAccessId)) {
        LOG.debug("Malformed s3 header. awsAccessID is empty");
        throw new S3Exception("awsAccessID is empty", S3ErrorCode.ACCESS_DENIED_ERROR);
      }

      return new AwsAuthInfo(awsAccessId,
              stringToSign,
              signatureInfo.getSignature()
              );
    } catch (S3Exception ex) {
      LOG.debug("Error during signature parsing: ", ex);
      throw ex;
    } catch (Exception e) {
      // For any other critical errors during object creation throw Internal
      // error.
      LOG.debug("Error during signature parsing: ", e);
      throw new S3Exception(e, "Context is invalid", S3ErrorCode.INTERNAL_ERROR);
    }
  }
}
