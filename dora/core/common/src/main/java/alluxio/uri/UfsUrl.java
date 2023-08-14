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

package alluxio.uri;

import alluxio.AlluxioURI;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.UfsUrlMessage;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FilenameUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * This class represents a UFS URL in the Alluxio system. This {@link UfsUrl} represents the
 * absolute ufs path.
 */
public class UfsUrl {

  public static final String SCHEME_SEPARATOR = "://";
  public static final String DOUBLE_SLASH_SEPARATOR = "//";
  public static final String SLASH_SEPARATOR = "/";
  public static final String COLON_SEPARATOR = ":";

  private final UfsUrlMessage mProto;

  private static class Parser {
    private final String mScheme;
    private final String mAuthority;
    private final List<String> mPathComponents;

    private Parser(String inputUrl) {
      Preconditions.checkArgument(inputUrl != null && !inputUrl.isEmpty(),
          "The input url is null or empty, please input a valid url.");
      String scheme = null;
      String authority = null;
      String path = null;

      int start = 0;
      int schemeSplitIndex = inputUrl.indexOf(SCHEME_SEPARATOR, start);
      if (schemeSplitIndex == -1) {
        scheme = "";
      } else {
        scheme = inputUrl.substring(start, schemeSplitIndex);
        start += scheme.length();
      }
      Preconditions.checkArgument(!scheme.equalsIgnoreCase("alluxio"),
          "Alluxio 3.x no longer supports alluxio:// scheme,"
              + " please input the UFS path directly like hdfs://host:port/path");

      // unified address "://" and "//"
      while (start < inputUrl.length() && inputUrl.charAt(start) == COLON_SEPARATOR.charAt(0))  {
        start++;
      }

      if (inputUrl.startsWith(DOUBLE_SLASH_SEPARATOR, start)
          && start + DOUBLE_SLASH_SEPARATOR.length() < inputUrl.length()) {
        start += DOUBLE_SLASH_SEPARATOR.length();
        int authoritySplitIndex = inputUrl.indexOf(SLASH_SEPARATOR, start);
        if (authoritySplitIndex == -1)  {
          authority = inputUrl.substring(start);
        } else {
          authority = inputUrl.substring(start, authoritySplitIndex);
        }
      } else {
        authority = "";
      }

      start += authority.length();
      // remove the fronting slash, if any.
      while (start < inputUrl.length() && inputUrl.charAt(start) == SLASH_SEPARATOR.charAt(0)) {
        start++;
      }
      path = FilenameUtils.normalizeNoEndSeparator(inputUrl.substring(start));

      // scheme, authority, pathComponents are always not null.
      mScheme = scheme;
      mAuthority = authority;

      if (path.isEmpty()) {
        mPathComponents = new ArrayList<>();
      } else {
        mPathComponents = Arrays.asList(path.split(SLASH_SEPARATOR));
      }
    }

    public String getScheme() {
      return mScheme;
    }

    public String getAuthority() {
      return mAuthority;
    }

    public List<String> getPathComponents() {
      return mPathComponents;
    }
  }

  /**
   * Creates an UfsUrl instance from a UfsUrlMessage.
   * @param proto an UfsUrlMessage
   * @return an UfsUrl object
   */
  public static UfsUrl createInstance(UfsUrlMessage proto) {
    return new UfsUrl(proto);
  }

  /**
   * Creates an UfsUrl instance from a String.
   *
   * @param ufsPath an input String representing the ufsPath
   * @return an UfsUrl representing the input String
   */
  public static UfsUrl createInstance(String ufsPath) {
    Preconditions.checkArgument(ufsPath != null && !ufsPath.isEmpty(),
        "input path is null or empty");

    Parser parser = new Parser(ufsPath);
    // if not present, the builder will throw exception.
    return new UfsUrl(UfsUrlMessage.newBuilder()
        .setScheme(parser.getScheme())
        .setAuthority(parser.getAuthority())
        .addAllPathComponents(parser.getPathComponents())
        .build());
  }

  /**
   * Constructs an UfsUrl from an UfsUrlMessage.
   * @param proto the proto message
   * @return an UfsUrl object
   */
  public static UfsUrl fromProto(UfsUrlMessage proto) {
    return new UfsUrl(proto);
  }

  /**
   * Constructs an {@link UfsUrl} from components.
   * @param proto the proto of the UfsUrl
   */
  public UfsUrl(UfsUrlMessage proto) {
    mProto = proto;
  }

  /**
   * Constructs an {@link UfsUrl} from components.
   * @param scheme the scheme of the path
   * @param authority the authority of the path
   * @param path the path component of the UfsUrl
   */
  public UfsUrl(String scheme, String authority, String path) {
    String[] arrayOfPath = path.split(SLASH_SEPARATOR);
    List<String> pathComponentsList = Arrays.asList(arrayOfPath);
    mProto = UfsUrlMessage.newBuilder()
        .setScheme(scheme)
        .setAuthority(authority)
        .addAllPathComponents(pathComponentsList)
        .build();
  }

  /**
   * @return the scheme of the {@link UfsUrl}
   */
  public Optional<String> getScheme() {
    if (!mProto.hasScheme()) {
      return Optional.empty();
    }
    return Optional.of(mProto.getScheme());
  }

  /**
   * @return the authority of the {@link UfsUrl}
   */
  public Optional<Authority> getAuthority() {
    if (!mProto.hasAuthority()) {
      return Optional.empty();
    }
    return Optional.of(Authority.fromString(mProto.getAuthority()));
  }

  /**
   * @return the pathComponents List of the {@link UfsUrl}
   */
  // TODO(Tony Sun): In the future Consider whether pathComponents should be extracted as a class.
  public List<String> getPathComponents() {
    return mProto.getPathComponentsList();
  }

  /**
   * @return the proto field of the {@link UfsUrl}
   */
  public UfsUrlMessage getProto() {
    return mProto;
  }

  /**
   * @return the String representation of the {@link UfsUrl}
   */
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(mProto.getScheme());
    if (!mProto.getScheme().isEmpty()) {
      stringBuilder.append(SCHEME_SEPARATOR);
    } else {
      stringBuilder.append(DOUBLE_SLASH_SEPARATOR);
    }
    stringBuilder.append(mProto.getAuthority());
    stringBuilder.append(getFullPath());
    return stringBuilder.toString();
  }

  /**
   * @return hashCode of {@link UfsUrl}
   */
  public int hashCode() {
    return mProto.hashCode();
  }

  /**
   * Indicates whether some other object is "equal to" this one.
   *
   * @param o an object
   * @return true if equal, false if not equal
   */
  public boolean equals(Object o) {
    if (this == o)  {
      return true;
    }
    if (!(o instanceof UfsUrl)) {
      return false;
    }
    UfsUrl that = (UfsUrl) o;
    return mProto.equals(that.mProto);
  }

  /**
   * Gets parent UfsUrl of current UfsUrl.
   *
   * @return parent UfsUrl
   */
  // TODO(Jiacheng Liu): try to avoid the copy by a RelativeUrl class
  public UfsUrl getParentURL() {
    List<String> pathComponents = mProto.getPathComponentsList();
    return new UfsUrl(UfsUrlMessage.newBuilder()
        .setScheme(mProto.getScheme())
        .setAuthority(mProto.getAuthority())
        // TODO(Jiacheng Liu): how many copies are there. Improve the performance in the future.
        .addAllPathComponents(pathComponents.subList(0, pathComponents.size() - 1)).build());
  }

  /**
   * Gets a child UfsUrl of current UfsUrl.
   *
   * @param childName a child file/directory of this object
   * @return a child UfsUrl
   */
  // TODO(Jiacheng Liu): try to avoid the copy by a RelativeUrl class
  public UfsUrl getChildURL(String childName) {
    Preconditions.checkArgument(childName != null && !childName.isEmpty(),
        "The input string is null or empty, please input a valid file/dir name.");
    List<String> pathComponents = mProto.getPathComponentsList();
    return new UfsUrl(UfsUrlMessage.newBuilder()
        .setScheme(mProto.getScheme())
        .setAuthority(mProto.getAuthority())
        .addAllPathComponents(pathComponents).addPathComponents(childName).build());
  }

  /**
   * Returns the full path by connecting the pathComponents list.
   *
   * @return a full path string
   */
  public String getFullPath() {
    StringBuilder sb = new StringBuilder();
    sb.append(SLASH_SEPARATOR);
    int n = mProto.getPathComponentsList().size();
    // Then sb is not empty.
    for (int i = 0; i < n - 1; i++) {
      sb.append(mProto.getPathComponents(i));
      sb.append(SLASH_SEPARATOR);
    }
    if (n - 1 >= 0)  {
      sb.append(mProto.getPathComponents(n - 1));
    }
    return sb.toString();
  }

  /**
   * Translates current UfsUrl object to an AlluxioURI object.
   *
   * @return a corresponding AlluxioURI object
   */
  public AlluxioURI toAlluxioURI() {
    return new AlluxioURI(mProto.getScheme(),
        Authority.fromString(mProto.getAuthority()), getFullPath());
  }

  /**
   * Returns the number of elements of the path component of the {@link UfsUrl}.
   *
   * @return the depth
   */
  public int getDepth() {
    return getPathComponents().size();
  }

  /**
   * Gets the final component of the {@link UfsUrl}.
   *
   * @return the final component of the {@link UfsUrl}
   */
  public String getName() {
    List<String> pathComponents = getPathComponents();
    Preconditions.checkArgument(!pathComponents.isEmpty());
    return pathComponents.get(pathComponents.size() - 1);
  }

  /**
   * Returns true if the current UfsUrl is an ancestor of another UfsUrl.
   * otherwise, return false.
   * @param ufsUrl potential children to check
   * @return true the current ufsUrl is an ancestor of the ufsUrl
   */
  public boolean isAncestorOf(UfsUrl ufsUrl) throws InvalidPathException {
    if (!Objects.equals(getAuthority(), ufsUrl.getAuthority())) {
      return false;
    }
    if (!Objects.equals(getScheme(), ufsUrl.getScheme())) {
      return false;
    }
    // TODO(Tony Sun): optimize the performance later
    // Both of the ufsUrls has the same scheme and authority, so just need to compare their paths.
    return PathUtils.hasPrefix(PathUtils.normalizePath(ufsUrl.getFullPath(), SLASH_SEPARATOR),
        PathUtils.normalizePath(getFullPath(), SLASH_SEPARATOR));
  }

  /**
   * Appends additional path elements to the end of an {@link UfsUrl}.
   *
   * @param suffix the suffix to add
   * @return the new {@link UfsUrl}
   */
  public UfsUrl join(String suffix) {
    if (suffix == null || suffix.isEmpty()) {
      return new UfsUrl(mProto);
    }
    String[] suffixArray = suffix.split(SLASH_SEPARATOR);
    int nonEmptyIndex = 0;
    while (nonEmptyIndex < suffixArray.length && suffixArray[nonEmptyIndex].isEmpty())  {
      nonEmptyIndex++;
    }
    List<String> suffixComponentsList;
    if (nonEmptyIndex == 0) {
      suffixComponentsList = Arrays.asList(suffixArray);
    } else {
      suffixComponentsList = Arrays.asList(
          Arrays.copyOfRange(
              suffixArray,
              nonEmptyIndex, suffixArray.length));
    }
    List<String> pathComponents = mProto.getPathComponentsList();
    return new UfsUrl(UfsUrlMessage.newBuilder()
        .setScheme(mProto.getScheme())
        .setAuthority(mProto.getAuthority())
        .addAllPathComponents(pathComponents)
        .addAllPathComponents(suffixComponentsList).build());
  }
}
