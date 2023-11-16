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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * This class represents a UFS URL in the Alluxio system.
 * <p>
 * A UFS URL:
 * <ul>
 *   <li><b>always</b> has a scheme that is not CONSTANTS SCHEME, e.g. {@code s3a:};</li>
 *   <li><b>optionally,</b> can have an authority.
 *   The authority can be made up of a hostname and a port,
 *   e.g. {@code localhost:80}, but can be an arbitrary string,
 *   e.g. a bucket name in an object store;
 *   </li>
 *   <li><b>always</b> has an absolute path.
 *   A path is a list of path segments that are separated by {@code /}.
 *   </li>
 * </ul>
 * Examples of valid UFS URL's:
 * <ul>
 *   <li>s3a://bucket1/file</li>
 *   <li>hdfs://namenode:9083/</li>
 *   <li>file:///home/user/file</li>
 *   <li>foo://bar boo:8080/abc/c</li>
 * </ul>
 * Examples of invalid UFS URL's:
 * <ul>
 *   <li>alluxio://localhost:19998/</li>
 *   <li>/opt/alluxio</li>
 * </ul>
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
      int schemeSplitIndex = inputUrl.indexOf(COLON_SEPARATOR, start);
      if (schemeSplitIndex == -1) {
        scheme = "";
      } else {
        scheme = inputUrl.substring(start, schemeSplitIndex);
        start += scheme.length();
      }
      Preconditions.checkArgument(!scheme.isEmpty(), "scheme is not allowed to be empty,"
          + "please input again.");
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
      // TODO(Tony Sun): remove the copy in next pr
      path = FilenameUtils.normalizeNoEndSeparator(inputUrl.substring(start));

      // scheme, authority, pathComponents are always not null.
      mScheme = scheme;
      mAuthority = authority;

      if (path.isEmpty()) {
        mPathComponents = Collections.emptyList();
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
  private UfsUrl(UfsUrlMessage proto) {
    Preconditions.checkArgument(!proto.getScheme().isEmpty(), "scheme is not allowed to be empty,"
        + "please input again.");
    Preconditions.checkArgument(!proto.getScheme().equalsIgnoreCase("alluxio"),
        "Alluxio 3.x no longer supports alluxio:// scheme,"
            + " please input the UFS path directly like hdfs://host:port/path");
    mProto = proto;
  }

  /**
   * Constructs an {@link UfsUrl} from components.
   * @param scheme the scheme of the path
   * @param authority the authority of the path
   * @param path the path component of the UfsUrl
   */
  public UfsUrl(String scheme, String authority, String path) {
    Preconditions.checkArgument(!scheme.isEmpty(), "scheme is not allowed to be empty,"
        + "please input again.");
    Preconditions.checkArgument(!scheme.equalsIgnoreCase("alluxio"),
        "Alluxio 3.x no longer supports alluxio:// scheme,"
            + " please input the UFS path directly like hdfs://host:port/path");
    String[] arrayOfPath = path.split(SLASH_SEPARATOR);
    int notEmpty = 0;
    while (notEmpty < arrayOfPath.length && arrayOfPath[notEmpty].isEmpty()) {
      notEmpty++;
    }
    List<String> pathComponentsList = Arrays.asList(arrayOfPath);
    mProto = UfsUrlMessage.newBuilder()
        .setScheme(scheme)
        .setAuthority(authority)
        .addAllPathComponents(pathComponentsList.subList(notEmpty, pathComponentsList.size()))
        .build();
  }

  /**
   * @return the scheme of the {@link UfsUrl}
   */
  public String getScheme() {
    Preconditions.checkArgument(mProto.hasScheme(), "The UfsUrl has no scheme, it is illegal.");
    return mProto.getScheme();
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
  public UfsUrlMessage toProto() {
    return mProto;
  }

  /**
   * return the UfsUrl String representation.
   * <p>
   * e.g.
   * <ul>
   *   <li>scheme="abc", authority="localhost:19998", path="/d/e/f",
   *   toString() -> "abc://localhost:19998/d/e/f".
   *   </li>
   *   <li>scheme="file", authority="", path="/testDir/testFile",
   *   toString() -> "file:///testDir/testFile".
   *   </li>
   * </ul>
   *
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
   * @return Optional UfsUrl The parent UfsUrl or null if at root
   */
  public Optional<UfsUrl> getParentURL() {
    if (mProto.getPathComponentsList().isEmpty()) {
      return Optional.empty();
    }
    List<String> pathComponents = mProto.getPathComponentsList();
    return Optional.of(new UfsUrl(UfsUrlMessage.newBuilder()
        .setScheme(mProto.getScheme())
        .setAuthority(mProto.getAuthority())
        // sublist returns a view, not a copy
        .addAllPathComponents(pathComponents.subList(0, pathComponents.size() - 1)).build()));
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
    // handle the special case "/"
    if (pathComponents.isEmpty()) {
      return "";
    }
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
      // TODO(Tony Sun): optimize the performance later
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
