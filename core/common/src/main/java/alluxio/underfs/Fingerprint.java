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

import alluxio.security.authorization.AccessControlList;

import com.google.common.base.Splitter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.Nullable;

/**
 * Fingerprint for a UFS file or directory.
 */
@NotThreadSafe
public final class Fingerprint {
  /** These tags are required in all fingerprints. */
  private static final Tag[] REQUIRED_TAGS = {Tag.TYPE, Tag.UFS, Tag.OWNER, Tag.GROUP, Tag.MODE};
  /** These tags are optional, and are serialized after the required tags. */
  private static final Tag[] OPTIONAL_TAGS = {Tag.CONTENT_HASH, Tag.ACL};

  /** These tags are all the metadata tags in the fingerprints. */
  private static final Tag[] METADATA_TAGS = {Tag.OWNER, Tag.GROUP, Tag.MODE, Tag.ACL};
  /** These tags are all the content tags in the fingerprints. */
  private static final Tag[] CONTENT_TAGS = {Tag.TYPE, Tag.UFS, Tag.CONTENT_HASH};

  private static final char KVDELIMTER = '|';
  private static final char TAGDELIMTER = ' ';

  private static final Pattern SANITIZE_REGEX = Pattern.compile("[" + KVDELIMTER
      + TAGDELIMTER + "]");
  public static final String UNDERSCORE = "_";

  private final Map<Tag, String> mValues;

  /**
   * The possible types of the fingerprint.
   */
  public enum Type {
    FILE,
    DIRECTORY,
  }

  /**
   * The possible tags within the fingerprint.
   */
  public enum Tag {
    TYPE,
    UFS,
    OWNER,
    GROUP,
    MODE,
    CONTENT_HASH,
    ACL,
  }

  /**
   * Parses the input string and returns the fingerprint object.
   *
   * @param ufsName the name of the ufs, should be {@link UnderFileSystem#getUnderFSType()}
   * @param status the {@link UfsStatus} to create the fingerprint from
   * @return the fingerprint object
   */
  public static Fingerprint create(String ufsName, UfsStatus status) {
    if (status == null) {
      return new Fingerprint(Collections.emptyMap());
    }
    return new Fingerprint(Fingerprint.createTags(ufsName, status));
  }

  /**
   * Parses the input string and returns the fingerprint object.
   *
   * @param ufsName the name of the ufs, should be {@link UnderFileSystem#getUnderFSType()}
   * @param status the {@link UfsStatus} to create the fingerprint from
   * @param acl the {@link AccessControlList} to create the fingerprint from
   * @return the fingerprint object
   */
  public static Fingerprint create(String ufsName, UfsStatus status, AccessControlList acl) {
    if (status == null) {
      return new Fingerprint(Collections.emptyMap());
    }
    Map<Tag, String> tagMap = Fingerprint.createTags(ufsName, status);
    if (acl != null) {
      tagMap.put(Tag.ACL, acl.toString());
    }
    return new Fingerprint(tagMap);
  }

  /**
   * Parses the input status and returns a tag map.
   *
   * @param ufsName the name of the ufs, should be {@link UnderFileSystem#getUnderFSType()}
   * @param status the {@link UfsStatus} to create the tagmap from
   * @return the tag map object
   */
  private static Map<Tag, String> createTags(String ufsName, UfsStatus status) {
    Map<Tag, String> tagMap = new HashMap<>();
    tagMap.put(Tag.UFS, ufsName);
    tagMap.put(Tag.OWNER, status.getOwner());
    tagMap.put(Tag.GROUP, status.getGroup());
    tagMap.put(Tag.MODE, String.valueOf(status.getMode()));
    if (status instanceof UfsFileStatus) {
      tagMap.put(Tag.TYPE, Type.FILE.name());
      tagMap.put(Tag.CONTENT_HASH, ((UfsFileStatus) status).getContentHash());
    } else {
      tagMap.put(Tag.TYPE, Type.DIRECTORY.name());
    }
    return tagMap;
  }

  /**
   * Parses the input string and returns the fingerprint object.
   *
   * @param input the string to parse
   * @return the parsed fingerprint object, or null if parsing failed
   */
  @Nullable
  public static Fingerprint parse(String input) {
    if (input == null) {
      return null;
    }
    if (Constants.INVALID_UFS_FINGERPRINT.equals(input)) {
      return new Fingerprint(Collections.emptyMap());
    }
    Map<String, String> kv = Splitter.on(TAGDELIMTER).trimResults().omitEmptyStrings()
            .withKeyValueSeparator(KVDELIMTER).split(input);
    Map<Tag, String> tagMap = new HashMap<>();

    for (Map.Entry<String, String> entry : kv.entrySet()) {
      tagMap.put(Tag.valueOf(entry.getKey()), entry.getValue());
    }

    // Verify required tags
    for (Tag tag : REQUIRED_TAGS) {
      if (!tagMap.containsKey(tag)) {
        return null;
      }
    }

    return new Fingerprint(tagMap);
  }

  /**
   * Checks if the fingerprint object was generated from an INVALID_UFS_FINGERPRINT.
   *
   * @return returns true if the fingerprint is valid
   */
  public boolean isValid() {
    if (mValues.isEmpty()) {
      return false;
    }

    // Check required tags
    for (Tag tag : REQUIRED_TAGS) {
      if (!mValues.containsKey(tag)) {
        return false;
      }
    }

    return true;
  }

  /**
   * @return the serialized string of the fingerprint
   */
  public String serialize() {
    if (mValues.isEmpty()) {
      return Constants.INVALID_UFS_FINGERPRINT;
    }
    StringBuilder sb = new StringBuilder();
    // Serialize required tags first
    for (Tag tag : REQUIRED_TAGS) {
      sb.append(tag).append(KVDELIMTER).append(getTag(tag)).append(TAGDELIMTER);
    }
    // Serialize optional tags last
    for (Tag tag : OPTIONAL_TAGS) {
      if (mValues.containsKey(tag)) {
        sb.append(tag).append(KVDELIMTER).append(getTag(tag)).append(TAGDELIMTER);
      }
    }
    return sb.toString();
  }

  /**
   * Returns true if the serialized fingerprint matches the fingerprint in metadata.
   *
   * @param fp a parsed fingerprint object
   * @return true if the given fingerprint matches the current fingerprint in metadata
   */
  public boolean matchMetadata(Fingerprint fp) {
    for (Tag tag : METADATA_TAGS) {
      if (!getTag(tag).equals(fp.getTag(tag))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns true if the serialized fingerprint matches the fingerprint in the content part.
   *
   * @param fp a parsed fingerprint object
   * @return true if the given fingerprint matches the current fingerprint's content
   */
  public boolean matchContent(Fingerprint fp) {
    for (Tag tag : CONTENT_TAGS) {
      if (!getTag(tag).equals(fp.getTag(tag))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Updates a specific tag in the fingerprint. If the new value is null, the fingerprint is kept
   * unchanged.
   *
   * @param tag the tag to update
   * @param value the new value of the tag
   */
  public void putTag(Tag tag, String value) {
    if (value != null) {
      mValues.put(tag, sanitizeString(value));
    }
  }

  /**
   * @param tag the tag to get
   * @return the value of the tag
   */
  public String getTag(Tag tag) {
    String value = mValues.get(tag);
    if (value == null) {
      return UNDERSCORE;
    }
    return value;
  }

  private Fingerprint(Map<Tag, String> values) {
    mValues = new HashMap<>();
    for (Map.Entry<Tag, String> entry : values.entrySet()) {
      putTag(entry.getKey(), entry.getValue());
    }
  }

  private String sanitizeString(String input) {
    if (input == null || input.isEmpty()) {
      return UNDERSCORE;
    }
    return SANITIZE_REGEX.matcher(input).replaceAll(UNDERSCORE);
  }
}
