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

import static alluxio.proxy.s3.signature.SignerConstants.X_AMZ_DATE;

import alluxio.proxy.s3.S3ErrorCode;
import alluxio.proxy.s3.S3Exception;
import alluxio.proxy.s3.auth.AwsAuthInfo;
import alluxio.proxy.s3.signature.utils.AwsAuthV2HeaderParserUtils;
import alluxio.proxy.s3.signature.utils.AwsAuthV4HeaderParserUtils;
import alluxio.proxy.s3.signature.utils.AwsAuthV4QueryParserUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  /**
   * Create a new {@link AwsSignatureProcessor}.
   *
   * @param context ContainerRequestContext
   */
  public AwsSignatureProcessor(ContainerRequestContext context) {
    mContext = context;
  }

  /**
   * Extract signature info from request.
   * @return SignatureInfo
   * @throws S3Exception
   */
  public SignatureInfo parseSignature() throws S3Exception {
    LowerCaseKeyStringMap headers =
        LowerCaseKeyStringMap.fromHeaderMap(mContext.getHeaders());

    String authHeader = headers.get(AUTHORIZATION);
    String dateHeader = headers.get(X_AMZ_DATE);
    Map<String, String> queryParameters = StringToSignProducer.fromMultiValueToSingleValueMap(
            mContext.getUriInfo().getQueryParameters());

    SignatureInfo signatureInfo = null;
    if ((signatureInfo =
            AwsAuthV2HeaderParserUtils.parseSignature(authHeader)) != null
        || (signatureInfo =
            AwsAuthV4HeaderParserUtils.parseSignature(authHeader, dateHeader)) != null
        || (signatureInfo =
            AwsAuthV4QueryParserUtils.parseSignature(queryParameters)) != null) {
      return signatureInfo;
    } else {
      return new SignatureInfo(
              SignatureInfo.Version.NONE,
              "", "", "", "", "", "", "", false
      );
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
        stringToSign =
                StringToSignProducer.createSignatureBase(signatureInfo, mContext);
      }
      String awsAccessId = signatureInfo.getAwsAccessId();
      // ONLY validate aws access id when needed.
      if (awsAccessId == null || awsAccessId.equals("")) {
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

  /**
   * A simple map which forces lower case key usage.
   */
  public static class LowerCaseKeyStringMap implements Map<String, String> {
    private Map<String, String> mDelegate;

    /**
     * Constructs a new {@link LowerCaseKeyStringMap}.
     */
    public LowerCaseKeyStringMap() {
      mDelegate = new HashMap<>();
    }

    /**
     * Constructs a new {@link LowerCaseKeyStringMap} from headers.
     * @param rawHeaders
     * @return a LowerCaseKeyStringMap instance
     */
    public static LowerCaseKeyStringMap fromHeaderMap(
        MultivaluedMap<String, String> rawHeaders) {

      //header map is MUTABLE. It's better to save it here. (with lower case
      // keys!!!)
      final LowerCaseKeyStringMap headers = new LowerCaseKeyStringMap();

      for (Entry<String, List<String>> headerEntry : rawHeaders.entrySet()) {
        if (0 < headerEntry.getValue().size()) {
          String headerKey = headerEntry.getKey();
          if (headers.containsKey(headerKey)) {
            //multiple headers from the same type are combined
            headers.put(headerKey,
                    String.format("%s,%s", headers.get(headerKey), headerEntry.getValue().get(0)));
          } else {
            headers.put(headerKey, headerEntry.getValue().get(0));
          }
        }
      }

      if (LOG.isTraceEnabled()) {
        headers.keySet().forEach(k -> LOG.trace("Header:{},value:{}", k,
             headers.get(k)));
      }
      return headers;
    }

    @Override
    public int size() {
      return mDelegate.size();
    }

    @Override
    public boolean isEmpty() {
      return mDelegate.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
      return mDelegate.containsKey(key.toString().toLowerCase());
    }

    @Override
    public boolean containsValue(Object value) {
      return mDelegate.containsValue(value);
    }

    @Override
    public String get(Object key) {
      return mDelegate.get(key.toString().toLowerCase());
    }

    @Override
    public String put(String key, String value) {
      return mDelegate.put(key.toLowerCase(), value);
    }

    @Override
    public String remove(Object key) {
      return mDelegate.remove(key.toString());
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
      for (Entry<? extends String, ? extends String> entry : m.entrySet()) {
        put(entry.getKey().toLowerCase(), entry.getValue());
      }
    }

    @Override
    public void clear() {
      mDelegate.clear();
    }

    @Override
    public Set<String> keySet() {
      return mDelegate.keySet();
    }

    @Override
    public Collection<String> values() {
      return mDelegate.values();
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
      return mDelegate.entrySet();
    }
  }
}
