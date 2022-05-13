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

import static alluxio.proxy.s3.S3Constants.AUTHORIZATION_CHARSET;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.SigningAlgorithm;
import org.apache.commons.lang3.StringUtils;
import org.apache.kerby.util.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.GeneralSecurityException;

/**
 * AWS v4 authentication payload validator. For more details refer to AWS
 * documentation https://docs.aws.amazon.com/general/latest/gr/
 * sigv4-create-canonical-request.html.
 **/
public class AuthorizationV4Validator {
  private static final Logger LOG =
            LoggerFactory.getLogger(AuthorizationV4Validator.class);

  private static final char LINE_SEPARATOR = '\n';
  private static final char SIGN_SEPARATOR = '/';
  private static final String AWS4_TERMINATOR = "aws4_request";
  private static final SigningAlgorithm HMAC_SHA256 = SigningAlgorithm.HmacSHA256;

  private AuthorizationV4Validator() {
  }

  /**
   * Validate request by comparing Signature from request. Returns true if
   * aws request is legit else returns false.
   * Signature = HEX(HMAC_SHA256(key, String to Sign))
   *
   * For more details refer to AWS documentation: https://docs.aws.amazon.com
   * /AmazonS3/latest/API/sigv4-streaming.html
   *
   * @param strToSign string to sign
   * @param signature signature string
   * @param userKey secretKey
   * @return ture, if validate success
   */
  public static boolean validateRequest(String strToSign, String signature,
                                        String userKey) {
    String expectedSignature = Hex.encode(sign(getSignedKey(userKey,
            strToSign), strToSign));
    return expectedSignature.equals(signature);
  }

  /**
   * function to compute sign.
   * @param key key info
   * @param msg msg info
   * @return sign bytes
   */
  private static byte[] sign(byte[] key, String msg) {
    return sign(msg.getBytes(AUTHORIZATION_CHARSET), key, HMAC_SHA256);
  }

  private static byte[] sign(byte[] data, byte[] key, SigningAlgorithm algorithm)
          throws SdkClientException {
    try {
      Mac mac = algorithm.getMac();
      mac.init(new SecretKeySpec(key, algorithm.name()));
      return mac.doFinal(data);
    } catch (GeneralSecurityException gse) {
      throw new RuntimeException(gse);
    }
  }

  /**
   * Returns signed key.
   *
   * @param key
   * @param strToSign
   *
   * SignatureKey = HMAC-SHA256(HMAC-SHA256(HMAC-SHA256(HMAC-SHA256("AWS4" +
   * "<YourSecretAccessKey/>","20130524"),"us-east-1"),"s3"),"aws4_request")
   *
   * For more details refer to AWS documentation: https://docs.aws.amazon
   * .com/AmazonS3/latest/API/sig-v4-header-based-auth.html
   *
   * */
  private static byte[] getSignedKey(String key, String strToSign) {
    LOG.debug("strToSign:{}", strToSign);
    String[] signData = StringUtils.split(StringUtils.split(strToSign,
            LINE_SEPARATOR)[2],
            SIGN_SEPARATOR);
    String dateStamp = signData[0];
    String regionName = signData[1];
    String serviceName = signData[2];
    byte[] kDate = sign(String.format("%s%s", "AWS4", key)
                .getBytes(AUTHORIZATION_CHARSET), dateStamp);
    byte[] kRegion = sign(kDate, regionName);
    byte[] kService = sign(kRegion, serviceName);
    byte[] kSigning = sign(kService, AWS4_TERMINATOR);
    LOG.info(Hex.encode(kSigning));
    return kSigning;
  }
}
