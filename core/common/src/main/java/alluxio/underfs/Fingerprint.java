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

package alluxio.underfs;

import alluxio.Constants;

import com.google.common.base.Splitter;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Fingerprint for a UFS file or directory.
 */
@NotThreadSafe
public abstract class Fingerprint {
  private static final String TAG_TYPE = "type";
  private static final String TAG_UFS = "ufs";
  private static final String TAG_OWNER = "owner";
  private static final String TAG_GROUP = "group";
  private static final String TAG_MODE = "mode";

  private static final String[] TAGS = {TAG_TYPE, TAG_UFS, TAG_OWNER, TAG_GROUP, TAG_MODE};

  private static final Pattern SANITIZE_REGEX = Pattern.compile("[ :]");

  private final Map<String, String> mValues;

  /**
   * Returns the ufs name for fingerprints.
   *
   * @param ufs the {@link UnderFileSystem} to get the name for
   * @return the name of the UFS, for fingerprints
   */
  public static String getUfsName(UnderFileSystem ufs) {
    return ufs.getClass().getSimpleName();
  }

  /**
   * Parses the input string and returns the fingerprint object.
   *
   * @param ufsName the name of the ufs, calculated by {@link #getUfsName(UnderFileSystem)}
   * @param status the {@link UfsStatus} to create the fingerprint from
   * @return the fingerprint object
   */
  public static Fingerprint create(String ufsName, UfsStatus status) {
    if (status == null) {
      return new InvalidFingerprint();
    }
    if (status instanceof UfsFileStatus) {
      UfsFileStatus ufsFile = (UfsFileStatus) status;
      return new FileFingerprint(ufsName, ufsFile.getOwner(), ufsFile.getGroup(),
          String.valueOf(ufsFile.getMode()), ufsFile.getContentHash());
    } else {
      return new DirectoryFingerprint(ufsName, status.getOwner(), status.getGroup(),
          String.valueOf(status.getMode()));
    }
  }

  /**
   * Parses the input string and returns the fingerprint object.
   *
   * @param input the string to parse
   * @return the parsed fingerprint object, or null if parsing failed
   */
  public static Fingerprint parse(String input) {
    if (input == null) {
      return null;
    }
    if (Constants.INVALID_UFS_FINGERPRINT.equals(input)) {
      return new InvalidFingerprint();
    }
    Map<String, String> kv =
        Splitter.on(' ').trimResults().omitEmptyStrings().withKeyValueSeparator(':').split(input);

    String type = kv.get(TAG_TYPE);
    if (type == null) {
      return null;
    }

    if (type.equals(FileFingerprint.FINGERPRINT_TYPE)) {
      return new FileFingerprint(kv);
    } else if (type.equals(DirectoryFingerprint.FINGERPRINT_TYPE)) {
      return new DirectoryFingerprint(kv);
    } else {
      return null;
    }
  }

  /**
   * @return the serialized string of the fingerprint
   */
  public String serialize() {
    return toString();
  }

  /**
   * Creates new instance of {@link Fingerprint}.
   *
   * @param ufsName name of the ufs
   * @param type type of fingerprint
   * @param owner of the file
   * @param group of the file
   * @param mode of the file
   */
  Fingerprint(String ufsName, String type, String owner, String group, String mode) {
    mValues = new HashMap<>();
    putTag(TAG_TYPE, type);
    putTag(TAG_UFS, ufsName);
    putTag(TAG_OWNER, owner);
    putTag(TAG_GROUP, group);
    putTag(TAG_MODE, mode);
  }

  Fingerprint(Map<String, String> values) {
    mValues = new HashMap<>();
    for (Map.Entry<String, String> entry : values.entrySet()) {
      putTag(entry.getKey(), entry.getValue());
    }
  }

  void putTag(String tag, String value) {
    mValues.put(tag, sanitizeString(value));
  }

  String getTag(String tag) {
    String value = mValues.get(tag);
    if (value == null) {
      return "_";
    }
    return value;
  }

  String sanitizeString(String input) {
    if (input == null || input.isEmpty()) {
      return "_";
    }
    return SANITIZE_REGEX.matcher(input).replaceAll("_");
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (String tag : TAGS) {
      sb.append(tag).append(':').append(getTag(tag)).append(' ');
    }
    return sb.toString();
  }
}
