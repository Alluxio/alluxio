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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.proxy.s3.auth.Authenticator;
import alluxio.proxy.s3.auth.AwsAuthInfo;
import alluxio.proxy.s3.signature.AwsSignatureProcessor;
import alluxio.security.authentication.AuthType;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Collections;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.ext.Provider;

/**
 * Authentication filter for S3 API.
 * It will convert the content of the Authorization Header field to the user name.
 */
@PreMatching
@Provider
public class S3AuthenticationFilter implements ContainerRequestFilter {

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    if (!requestContext.getUriInfo().getPath().startsWith(S3RestServiceHandler.SERVICE_PREFIX)) {
      return;
    }
    String user = "N/A";
    try {
      String authorization = requestContext.getHeaderString("Authorization");
      user = getUser(authorization, requestContext);
      // replace the authorization header value to user
      requestContext.getHeaders().replace("Authorization", Collections.singletonList(user));
    } catch (Exception e) {
      requestContext.abortWith(S3ErrorResponse.createErrorResponse(e, "Authorization"));
    }
  }

  /**
   * Get username from header info.
   *
   * @param authorization authorization info
   * @return username
   * @throws S3Exception
   */
  private static String getUser(String authorization, ContainerRequestContext requestContext)
      throws S3Exception {
    // TODO(czhu): refactor PropertyKey.S3_REST_AUTHENTICATION_ENABLED to an ENUM
    //             to specify between using custom Authenticator class vs. Alluxio Master schemes
    if (S3RestUtils.isAuthenticationEnabled(Configuration.global())) {
      return getUserFromSignature(requestContext);
    }
    try {
      return getUserFromAuthorization(authorization, Configuration.global());
    } catch (RuntimeException e) {
      throw new S3Exception(new S3ErrorCode(S3ErrorCode.INTERNAL_ERROR.getCode(),
          e.getMessage(), S3ErrorCode.INTERNAL_ERROR.getStatus()));
    }
  }

  /**
   * Get username from parsed header info.
   *
   * @return username
   * @throws S3Exception
   */
  private static String getUserFromSignature(ContainerRequestContext requestContext)
      throws S3Exception {
    AwsSignatureProcessor signatureProcessor = new AwsSignatureProcessor(requestContext);
    Authenticator authenticator = Authenticator.Factory.create(Configuration.global());
    AwsAuthInfo authInfo = signatureProcessor.getAuthInfo();
    if (authenticator.isAuthenticated(authInfo)) {
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
    if (creds.length < 2 || !StringUtils.equals("Credential", creds[0])) {
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
