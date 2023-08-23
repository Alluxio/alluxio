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
import javax.annotation.Nullable;

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

      int firstSlash = inputUrl.indexOf(SLASH_SEPARATOR);
      int firstColon = inputUrl.indexOf(COLON_SEPARATOR);

      if (firstColon != -1) {
        if (firstSlash == -1) {
          // have colon, while have no slash
          scheme = inputUrl.substring(0, firstColon);
          start = firstColon + 1;
        } else if (firstColon < firstSlash) {
          // have colon and slash, colon is in front of slash -> have scheme
          start = firstSlash;
          scheme = inputUrl.substring(0, firstColon);
        } else { // have colon and slash, colon is back of slash -> illegal, have empty scheme
          scheme = "";
        }
      } else {
        scheme = "";
      }

      Preconditions.checkArgument(!scheme.isEmpty(), "empty scheme: %s", inputUrl);
      Preconditions.checkArgument(!scheme.equalsIgnoreCase("alluxio"),
          "Alluxio 3.x no longer supports alluxio:// scheme,"
              + " please input the UFS path directly like hdfs://host:port/path");

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
      // TODO(Tony Sun): remove the copy in the future
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

    /**
     * Normalize the path component of the {@link UfsUrl}, by replacing all "//" with
     * "/", and trimming trailing slash from non-root path.
     * From AlluxioURI.normalizePath(String).
     *
     * @param path the path to normalize
     * @return the normalized path
     */
    public static String removeRedundantSlashes(String path) {
      StringBuilder sb = new StringBuilder(path.length());
      int i = 0;
      while (i < path.length()) {
        if (path.charAt(i) != SLASH_SEPARATOR.charAt(0))  {
          sb.append(path.charAt(i));
          i++;
          continue;
        }
        sb.append(SLASH_SEPARATOR);
        // remove adjacent slashes
        while (i < path.length()) {
          if (path.charAt(i) != SLASH_SEPARATOR.charAt(0))  {
            break;
          }
          i++;
        }
      }
      return sb.toString();
    }

    public static String normalizePath(String path) {
      return path;
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
   *
   * @param proto the proto of the UfsUrl
   */
  private UfsUrl(UfsUrlMessage proto) {
    Preconditions.checkArgument(!proto.getScheme().isEmpty(), "scheme is not allowed to be empty,"
        + " please input again.");
    Preconditions.checkArgument(!proto.getScheme().equalsIgnoreCase("alluxio"),
        "Alluxio 3.x no longer supports alluxio:// scheme,"
            + " please input the UFS path directly like hdfs://host:port/path");
    mProto = proto;
  }

  /**
   * Constructs an {@link UfsUrl} from components.
   *
   * @param scheme    the scheme of the path
   * @param authority the authority of the path
   * @param path      the path component of the UfsUrl
   */
  public UfsUrl(String scheme, String authority, String path) {
    UfsUrl ufsUrl = new UfsUrl(scheme, authority, path, true);
    mProto = ufsUrl.toProto();
  }

  /**
   * Constructs an {@link UfsUrl} from components.
   *
   * @param scheme    the scheme of the path
   * @param authority the authority of the path
   * @param path      the path component of the UfsUrl
   * @param checkNormalization  the parameter to decide whether normalize the path string
   */
  public UfsUrl(String scheme, String authority, String path, boolean checkNormalization) {
    Preconditions.checkArgument(!scheme.isEmpty(), "empty scheme: %s",
        scheme + authority + path);
    Preconditions.checkArgument(!scheme.equalsIgnoreCase("alluxio"),
        "Alluxio 3.x no longer supports alluxio:// scheme,"
            + " please input the UFS path directly like hdfs://host:port/path");
    // TODO(Tony Sun): compose all the normalization function.
    path = Parser.removeRedundantSlashes(path);
    if (checkNormalization) {
      // TODO(Tony Sun): too many copies, remove it in the future
      java.net.URI uri = java.net.URI.create(path);
      path = uri.normalize().getPath();
    }

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
  public Authority getAuthority() {
    return Authority.fromString(mProto.getAuthority());
  }

  /**
   * @return the pathComponents List of the {@link UfsUrl}
   */
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
    StringBuilder stringBuilder = new StringBuilder(mProto.getScheme().length()
        + SCHEME_SEPARATOR.length() + mProto.getAuthority().length() + getFullPath().length());
    stringBuilder.append(mProto.getScheme());
    stringBuilder.append(SCHEME_SEPARATOR);
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
    if (this == o) {
      return true;
    }
    if (!(o instanceof UfsUrl)) {
      return false;
    }
    UfsUrl that = (UfsUrl) o;
    return mProto.equals(that.mProto);
  }

  /**
   * Gets parent UfsUrl of current UfsUrl or null if at root.
   * <p>
   * e.g.
   * <ul>
   *   <li>getParentURL(abc://1.2.3.4:19998/xy z/a b c) -> abc://1.2.3.4:19998/xy z</li>
   * </ul>
   *
   * @return parent UfsUrl or null if at root
   */
  @Nullable
  public UfsUrl getParentURL() {
    if (mProto.getPathComponentsList().size() == 0) {
      return null;
    }
    List<String> pathComponents = mProto.getPathComponentsList();
    return new UfsUrl(UfsUrlMessage.newBuilder()
        .setScheme(mProto.getScheme())
        .setAuthority(mProto.getAuthority())
        // sublist returns a view, not a copy
        .addAllPathComponents(pathComponents.subList(0, pathComponents.size() - 1)).build());
  }

  /**
   * Returns the full path by connecting the pathComponents list.
   *
   * @return a full path string
   */
  public String getFullPath() {
    // calculate the memory allocation first to reduce copies of StringBuilder
    int pathSize = 1;
    int n = mProto.getPathComponentsList().size();
    for (int i = 0; i < n - 1; i++) {
      pathSize += 1 + mProto.getPathComponents(i).length();
    }
    if (n - 1 >= 0) {
      pathSize += mProto.getPathComponents(n - 1).length();
    }

    StringBuilder sb = new StringBuilder(pathSize);

    sb.append(SLASH_SEPARATOR);
    // Then sb is not empty.
    for (int i = 0; i < n - 1; i++) {
      sb.append(mProto.getPathComponents(i));
      sb.append(SLASH_SEPARATOR);
    }
    if (n - 1 >= 0) {
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
    if (pathComponents.isEmpty()) {
      return "";
    }
    return pathComponents.get(pathComponents.size() - 1);
  }

  /**
   * Returns true if the current UfsUrl is an ancestor of another UfsUrl.
   * otherwise, return false.
   *
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
    return PathUtils.hasPrefix(ufsUrl.getFullPath(), this.getFullPath());
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
    while (nonEmptyIndex < suffixArray.length && suffixArray[nonEmptyIndex].isEmpty()) {
      nonEmptyIndex++;
    }
    List<String> suffixComponentsList = Arrays.asList(suffixArray);
    List<String> noEmptyElemSuffixComponentsList;
    if (nonEmptyIndex == 0) {
      noEmptyElemSuffixComponentsList = suffixComponentsList;
    } else {
      noEmptyElemSuffixComponentsList = suffixComponentsList.subList(nonEmptyIndex,
          suffixComponentsList.size());
    }
    List<String> pathComponents = mProto.getPathComponentsList();
    return new UfsUrl(UfsUrlMessage.newBuilder()
        .setScheme(mProto.getScheme())
        .setAuthority(mProto.getAuthority())
        .addAllPathComponents(pathComponents)
        .addAllPathComponents(noEmptyElemSuffixComponentsList).build());
  }

  /**
   * Append additional path elements to the end of an {@link UfsUrl}. This does not check if
   * the new path needs normalization, and is less CPU intensive than {@link #join(String)}.
   *
   * @param suffix the suffix to add
   * @return the new {@link UfsUrl}
   */
  public UfsUrl joinUnsafe(String suffix) {
    String path = getFullPath();
    StringBuilder sb = new StringBuilder(path.length() + 1 + suffix.length());
    return new UfsUrl(toProto().getScheme(), toProto().getAuthority(),
        sb.append(path).append(SLASH_SEPARATOR).append(suffix).toString(), false);
  }
}
