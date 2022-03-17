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

import org.apache.commons.lang3.StringUtils;
import org.apache.kerby.util.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * AWS v4 authentication payload validator. For more details refer to AWS
 * documentation https://docs.aws.amazon.com/general/latest/gr/
 * sigv4-create-canonical-request.html.
 **/
public class AWSV4AuthValidator {

  private static final Logger LOG =
            LoggerFactory.getLogger(AWSV4AuthValidator.class);
  private static final String HMAC_SHA256_ALGORITHM = "HmacSHA256";

  private AWSV4AuthValidator() {
  }

  /**
   * hash function.
   *
   * @param payload
   * @return hash string
   * @throws NoSuchAlgorithmException
   */
  public static String hash(String payload) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    md.update(payload.getBytes(StandardCharsets.UTF_8));
    return String.format("%064x", new java.math.BigInteger(1, md.digest()));
  }

  /**
   * function to compute sign.
   * @param key key info
   * @param msg msg info
   * @return sign bytes
   */
  private static byte[] sign(byte[] key, String msg) {
    try {
      SecretKeySpec signingKey = new SecretKeySpec(key, HMAC_SHA256_ALGORITHM);
      Mac mac = Mac.getInstance(HMAC_SHA256_ALGORITHM);
      mac.init(signingKey);
      return mac.doFinal(msg.getBytes(StandardCharsets.UTF_8));
    } catch (GeneralSecurityException gse) {
      throw new RuntimeException(gse);
    }
  }

  /**
   * Returns signing key.
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
  private static byte[] getSigningKey(String key, String strToSign) {
    LOG.info("strToSign:{}", strToSign);
    String[] signData = StringUtils.split(StringUtils.split(strToSign,
                '\n')[2], '/');
    String dateStamp = signData[0];
    String regionName = signData[1];
    String serviceName = signData[2];
    byte[] kDate = sign(("AWS4" + key)
                .getBytes(StandardCharsets.UTF_8), dateStamp);
    byte[] kRegion = sign(kDate, regionName);
    byte[] kService = sign(kRegion, serviceName);
    byte[] kSigning = sign(kService, "aws4_request");
    LOG.info(Hex.encode(kSigning));
    return kSigning;
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
    String expectedSignature = Hex.encode(sign(getSigningKey(userKey,
                strToSign), strToSign));
    return expectedSignature.equals(signature);
  }
}
