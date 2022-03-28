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

import alluxio.proxy.s3.S3ErrorCode;
import alluxio.proxy.s3.S3Exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedMap;
import java.util.ArrayList;
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

public class AWSSignatureProcessor implements SignatureProcessor {

  private static final Logger LOG =
            LoggerFactory.getLogger(AWSSignatureProcessor.class);

  private ContainerRequestContext mContext;

  /**
   * Create a new {@link AWSSignatureProcessor}.
   *
   * @param context ContainerRequestContext
   */
  public AWSSignatureProcessor(ContainerRequestContext context) {
    mContext = context;
  }

  @Override
  public SignatureInfo parseSignature() throws S3Exception {
    LowerCaseKeyStringMap headers =
        LowerCaseKeyStringMap.fromHeaderMap(mContext.getHeaders());

    String authHeader = headers.get(SignerConstants.AUTHORIZATION);

    List<SignatureParser> signatureParsers = new ArrayList<>();
    signatureParsers.add(new AuthorizationV4HeaderParser(authHeader,
                headers.get(SignerConstants.X_AMZ_DATE)));
    signatureParsers.add(new AuthorizationV4QueryParser(
        StringToSignProducer.fromMultiValueToSingleValueMap(
                mContext.getUriInfo().getQueryParameters())));
    signatureParsers.add(new AuthorizationV2HeaderParser(authHeader));

    SignatureInfo signatureInfo = null;
    for (SignatureParser parser : signatureParsers) {
      signatureInfo = parser.parseSignature();
      if (signatureInfo != null) {
        break;
      }
    }
    if (signatureInfo == null) {
      signatureInfo = new SignatureInfo(
             SignatureInfo.Version.NONE,
             "", "", "", "", "", "", "", false
      );
    }
    return signatureInfo;
  }

  @Override
  public SignedInfo getSignedInfo() throws S3Exception {
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
        LOG.debug("Malformed s3 header. awsAccessID: ", awsAccessId);
        throw new S3Exception("", S3ErrorCode.ACCESS_DENIED_ERROR);
      }

      return new SignedInfo(stringToSign,
              signatureInfo.getSignature(),
              awsAccessId);
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
            headers.put(headerKey, headers.get(headerKey) + "," + headerEntry.getValue().get(0));
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
