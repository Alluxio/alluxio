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
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.UfsUrlMessage;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FilenameUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * This class represents a UFS URL in the Alluxio system. This {@link UfsUrl} represents the
 * absolute ufs path.
 */
public class UfsUrl {

  public static final String SCHEME_SEPARATOR = "://";
  public static final String DOUBLE_SLASH_SEPARATOR = "//";
  public static final String PATH_SEPARATOR = "/";
  public static final String COLON_SEPARATOR = ":";

  final UfsUrlMessage mProto;

  /**
   * Constructs a map containing scheme, authority, and path.
   * @param inputUrl the input url string
   * @return a map containing scheme, authority, and path
   */
  public static Map<String, String> extractElements(String inputUrl) {
    Preconditions.checkArgument(inputUrl != null && !inputUrl.isEmpty(),
        "The input url is null or empty, please input a valid url.");
    Map<String, String> elements = new HashMap<>();
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
      int authoritySplitIndex = inputUrl.indexOf(PATH_SEPARATOR, start);
      if (authoritySplitIndex == -1)  {
        authority = inputUrl.substring(start);
      } else {
        authority = inputUrl.substring(start, authoritySplitIndex);
      }
    } else {
      authority = "";
    }

      start += authority.length();
      path = FilenameUtils.normalize(inputUrl.substring(start));

    elements.put("scheme", scheme);
    elements.put("authority", authority);
    elements.put("path", path);
    return elements;
  }

  /**
   * Determines the scheme of this absolute path object. The returns String is not empty.
   * @param rootUrl the elems map of root url, including scheme, authority and path
   * @param inputUrl the elems map of input url, including scheme, authority and path
   * @return the scheme of this UfsUrl object
   */
  public static String generateScheme(Map<String, String> rootUrl, Map<String, String> inputUrl) {
    if (rootUrl.get("scheme").isEmpty())  {
      if (inputUrl.get("scheme").isEmpty())  {
        return "file";
      } else {
        return inputUrl.get("scheme");
      }
    } else {  // rootUrl.scheme is not empty
      if (inputUrl.get("scheme").isEmpty())  {
        // means input is a relative path.
        return rootUrl.get("scheme");
      } else {
        return inputUrl.get("scheme");
      }
    }
  }

  /**
   * Determines the authority of this absolute path object.
   * @param rootUrl the elems map of root url, including scheme, authority and path
   * @param inputUrl the elems map of input url, including scheme, authority and path
   * @return the authority of this UfsUrl object
   */
  public static String generateAuthority(Map<String, String> rootUrl,
                                         Map<String, String> inputUrl) {
    if (rootUrl.get("authority").isEmpty())  {
      return  inputUrl.get("authority");
    } else {
      if (rootUrl.get("scheme").equals(inputUrl.get("scheme"))) {
        if (!inputUrl.get("authority").isEmpty()) {
          return inputUrl.get("authority");
        } else {
          return rootUrl.get("authority");
        }
      } else {
        return inputUrl.get("authority");
      }
    }
  }

  /**
   * Determines the path components list of this absolute path.
   * @param rootUrl the elems map of root url, including scheme, authority and path
   * @param inputUrl the elems map of input url, including scheme, authority and path
   * @return the path components list of this UfsUrl Object
   */
  public static List<String> generatePathComponents(Map<String, String> rootUrl,
                                                    Map<String, String> inputUrl) {
    List<String> inputPathComponents = Arrays.asList(inputUrl.get("path").split(PATH_SEPARATOR));

    if (rootUrl.equals(inputUrl)) {
      return inputPathComponents;
    }

    // inputUrl.scheme is "file" -> the inputUrl is probably a relative path.
    if (inputUrl.get("scheme").equals("file")) {
      // rootUrl.scheme is "file" or empty -> inputUrl is a relative path.
      if (inputUrl.get("scheme").equals(rootUrl.get("scheme")) || rootUrl.get("scheme").isEmpty()) {
        String concatPath = rootUrl.get("path") + inputUrl.get("path");
        return Arrays.asList(FilenameUtils
            .normalizeNoEndSeparator(concatPath).split(PATH_SEPARATOR));
      } else {  // inputUrl is an absolute path.
        return inputPathComponents;
      }
    } else {  // inputUrl is an absolute path.
      // if inputUrl.path is "/" or "//", i.e., root directory, set a "" to represent root.
      if (inputUrl.get("path").equals(PATH_SEPARATOR)
          || inputUrl.get("path").equals(DOUBLE_SLASH_SEPARATOR))  {
        inputPathComponents = Arrays.asList("");
      }
      return inputPathComponents;
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

    String ufsRootDir = Configuration.getString(PropertyKey.DORA_CLIENT_UFS_ROOT);

    Preconditions.checkArgument(ufsRootDir != null && !ufsRootDir.isEmpty(),
        "root dir is null or empty.");

    Map<String, String> rootDirElems = extractElements(ufsRootDir);
    Map<String, String> ufsPathElems = extractElements(ufsPath);

    Preconditions.checkArgument(
        rootDirElems.containsKey("scheme") && rootDirElems.get("scheme") != null
            && rootDirElems.containsKey("authority") && rootDirElems.get("authority") != null
            && rootDirElems.containsKey("path") && rootDirElems.get("path") != null);

    Preconditions.checkArgument(
        ufsPathElems.containsKey("scheme") && ufsPathElems.get("scheme") != null
            && ufsPathElems.containsKey("authority") && ufsPathElems.get("authority") != null
            && ufsPathElems.containsKey("path") && ufsPathElems.get("path") != null);

    String scheme = generateScheme(rootDirElems, ufsPathElems);
    Preconditions.checkArgument(!scheme.isEmpty(),
        "scheme is empty, please check input Url again.");
    ufsPathElems.put("scheme", scheme);
    if (scheme.equals("file"))  {
      rootDirElems.put("scheme", scheme);
    }

    String authority = generateAuthority(rootDirElems, ufsPathElems);
    ufsPathElems.put("authority", authority);
    List<String> pathComponents = generatePathComponents(rootDirElems, ufsPathElems);

    return new UfsUrl(UfsUrlMessage.newBuilder()
        .setScheme(scheme)
        .setAuthority(authority)
        .addAllPathComponents(pathComponents)
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
    Preconditions.checkArgument(proto.getPathComponentsList().size() != 0,
        "The proto.path is empty, please check the proto first.");
    mProto = proto;
  }

  /**
   * Constructs an {@link UfsUrl} from components.
   * @param scheme the scheme of the path
   * @param authority the authority of the path
   * @param path the path component of the UfsUrl
   */
  public UfsUrl(String scheme, String authority, String path) {
    String[] arrayOfPath = path.split(PATH_SEPARATOR);
    List<String> pathComponentsList = Arrays.asList(arrayOfPath);
    // handle root dir "/". If equal, pathComponentsList is empty, add "" to represent "/".
    if (path.equals(PATH_SEPARATOR))  {
      pathComponentsList.add("");
    }
    Preconditions.checkArgument(pathComponentsList.size() != 0,
        "The path is empty, please input a valid path");
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
    sb.append(PATH_SEPARATOR);
    // Then sb is not empty.
    for (int i = 1; i < mProto.getPathComponentsList().size(); i++) {
      sb.append(mProto.getPathComponentsList().get(i));
      if (!mProto.getPathComponentsList().get(i).isEmpty()) {
        sb.append(PATH_SEPARATOR);
      }
    }
    return FilenameUtils.normalizeNoEndSeparator(sb.toString());
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
    int pathComponentsSize = getPathComponents().size();
    Preconditions.checkArgument(pathComponentsSize > 0);
    // "/" is represented as an empty String "".
    return getPathComponents().get(0).isEmpty() ? pathComponentsSize - 1 : pathComponentsSize;
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
    return PathUtils.hasPrefix(PathUtils.normalizePath(ufsUrl.getFullPath(), PATH_SEPARATOR),
        PathUtils.normalizePath(getFullPath(), PATH_SEPARATOR));
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
    String[] suffixArray = suffix.split(PATH_SEPARATOR);
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
