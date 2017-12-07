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
  public static final Tag TAG_TYPE = new Tag("type");
  public static final Tag TAG_UFS = new Tag("ufs");
  public static final Tag TAG_OWNER = new Tag("owner");
  public static final Tag TAG_GROUP = new Tag("group");
  public static final Tag TAG_MODE = new Tag("mode");

  private static final Tag[] TAGS = {TAG_TYPE, TAG_UFS, TAG_OWNER, TAG_GROUP, TAG_MODE};

  private static final Pattern SANITIZE_REGEX = Pattern.compile("[ :]");

  private final Map<Tag, String> mValues;

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
    Map<Tag, String> tagMap = new HashMap<>();

    for (Map.Entry<String, String> entry : kv.entrySet()) {
      tagMap.put(new Tag(entry.getKey()), entry.getValue());
    }

    String type = tagMap.get(TAG_TYPE);
    if (type == null) {
      return null;
    }

    switch (type) {
      case FileFingerprint.FINGERPRINT_TYPE:
        return new FileFingerprint(tagMap);
      case DirectoryFingerprint.FINGERPRINT_TYPE:
        return new DirectoryFingerprint(tagMap);
      default:
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
   * Updates a specific tag in the fingerprint. If the new value is null, the fingerprint is kept
   * unchanged.
   *
   * @param tag the tag to update
   * @param value the new value of the tag
   */
  public void updateTag(Tag tag, String value) {
    if (value != null) {
      mValues.put(tag, sanitizeString(value));
    }
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

  Fingerprint(Map<Tag, String> values) {
    mValues = new HashMap<>();
    for (Map.Entry<Tag, String> entry : values.entrySet()) {
      putTag(entry.getKey(), entry.getValue());
    }
  }

  void putTag(Tag tag, String value) {
    mValues.put(tag, sanitizeString(value));
  }

  String getTag(Tag tag) {
    String value = mValues.get(tag);
    if (value == null) {
      return "_";
    }
    return value;
  }

  private String sanitizeString(String input) {
    if (input == null || input.isEmpty()) {
      return "_";
    }
    return SANITIZE_REGEX.matcher(input).replaceAll("_");
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (Tag tag : TAGS) {
      sb.append(tag).append(':').append(getTag(tag)).append(' ');
    }
    return sb.toString();
  }

  static final class Tag {
    private final String mName;

    Tag(String name) {
      mName = name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Tag)) {
        return false;
      }
      Tag that = (Tag) o;
      return mName.equals(that.mName);
    }

    @Override
    public int hashCode() {
      return mName.hashCode();
    }

    @Override
    public String toString() {
      return mName;
    }
  }
}
