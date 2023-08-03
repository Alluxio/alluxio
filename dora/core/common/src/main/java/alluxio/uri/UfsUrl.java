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
import alluxio.util.UfsUrlUtils;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.util.Strings;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class UfsUrl {

  public static final String SCHEME_SEPARATOR = "://";
  public static final String PATH_SEPARATOR = "/";
  public static final String PORT_SEPARATOR = ":";

  UfsUrlMessage mProto;

  public static UfsUrl createInstance(String ufsPath) {
    Preconditions.checkArgument(!ufsPath.isEmpty(),
        "ufsPath is empty, please input a non-empty ufsPath.");

    String rootDir = Configuration.getString(PropertyKey.DORA_CLIENT_UFS_ROOT);
    String rootScheme;
    String rootAuthority;
    String rootPath;
    // Please make sure the rootDir is not only contain scheme, like "s3://".
    String[] rootDirArray = rootDir.split(SCHEME_SEPARATOR);
    if (!ufsPath.equals(rootDir)) {
      // rootDir = "hdfs:///" -> rootDirArray = ["hdfs", "/"]
      Preconditions.checkArgument(rootDirArray.length <= 2, "Invalid Alluxio rootDir, "
          + "please check alluxio configuration first.");
      // rootDir is like "/tmp"
      if (rootDirArray.length == 1) {
        rootScheme = "file";
        rootAuthority = "";
        rootPath = rootDir;
      } else {
        // rootDirArray.length = 2. rootDirArray = [rootScheme, rootAuthAndPath]
        rootScheme = rootDirArray[0];
        int indexOfFirstColon = rootDirArray[1].indexOf(PORT_SEPARATOR);
        // There are no ':' in the authority and path. i.e., there are no authority.
        if (indexOfFirstColon == -1)  {
          rootAuthority = "";
          rootPath = rootDirArray[1];
        } else {
          int indexOfFirstSlash = rootDirArray[1].indexOf(PATH_SEPARATOR);
          rootAuthority = rootDirArray[1].substring(0, indexOfFirstSlash);
          rootPath = rootDirArray[1].substring(indexOfFirstSlash);
        }
      }
    } else {
      rootScheme = "";
      rootAuthority = "";
      rootPath = "";
    }

    int schemeIndex = ufsPath.indexOf(SCHEME_SEPARATOR);
    Preconditions.checkArgument(schemeIndex == ufsPath.lastIndexOf(SCHEME_SEPARATOR),
        "There are multiple schemes, the input may contain more than one path, "
            + "current UfsUrl only support inputting one path each time.");

    String scheme;
    String authority;
    String path;
    String authorityAndPath;
    // schemeIndex == -1 -> ufsPath without a scheme; schemeIndex != -1 -> ufsPath has scheme.
    if (schemeIndex == -1)  {
      // If without scheme, set default scheme to root scheme.
      if (rootDirArray.length == 1) {
        // if length == 1, means rootDirArray is not include root scheme too.
        // In this case, scheme is "file", i.e., local.
        scheme = "file";
      } else {
        // if length != 1, i.e. > 1, means rootDir has scheme.
        // And ufsPath has no scheme, so choose root scheme as scheme.
        scheme = rootDirArray[0];
      }
      authorityAndPath = ufsPath;
    } else {
      // ufsPath has one scheme.
      scheme = ufsPath.substring(0, schemeIndex);
      authorityAndPath = ufsPath.substring(schemeIndex + SCHEME_SEPARATOR.length());
    }
    Preconditions.checkArgument(!scheme.isEmpty(), "scheme is empty, please input again.");
    Preconditions.checkArgument(!authorityAndPath.isEmpty(),
        "authority or path is empty, please input again.");

    int indexOfFirstColon = authorityAndPath.indexOf(PORT_SEPARATOR);
    if (indexOfFirstColon == -1)  {
      // There are no ":", i.e., no authority.
      authority = rootAuthority;
      if (rootScheme.equalsIgnoreCase(scheme)) {
        // Handle case with two slash like "/tmp/" + "/cache" = "/tmp//cache"
        path = UfsUrlUtils.concatStringPath(rootPath, authorityAndPath);
      } else {
        // If false, means the root scheme is different from input, subject to input.
        path = authorityAndPath;
      }
    } else {
      // ufsPath has at least a ':', i.e., it has authority.
      int indexOfFirstSlash = authorityAndPath.indexOf(PATH_SEPARATOR);
      // If there are no '/' splitting authority and path, throw error.
      Preconditions.checkArgument(indexOfFirstSlash != -1,
          "The input has an authority while has no invalid path. Please input another one");
      authority = authorityAndPath.substring(0, indexOfFirstSlash);
      String tmpPath = authorityAndPath.substring(indexOfFirstSlash);
      if (rootScheme.equalsIgnoreCase(scheme))  {
        path = UfsUrlUtils.concatStringPath(rootPath, tmpPath);
      } else {
        path = tmpPath;
      }
    }
    return new UfsUrl(scheme, authority, path);
  }

  public static UfsUrl fromProto(UfsUrlMessage proto) {
    return new UfsUrl(proto);
  }

  public UfsUrl(UfsUrlMessage proto) {
    Preconditions.checkArgument(proto.getPathComponentsList().size() != 0,
        "The proto.path is empty, please check the proto first.");
    mProto = proto;
  }

  public UfsUrl(String scheme, String authority, String path) {
    String[] arrayOfPath = path.split(PATH_SEPARATOR);
    List<String> pathComponentsList = Arrays.asList(arrayOfPath);
    mProto = UfsUrlMessage.newBuilder()
        .setScheme(scheme)
        .setAuthority(authority)
        .addAllPathComponents(pathComponentsList)
        .build();
  }

  public Optional<String> getScheme() {
    if (!mProto.hasScheme()) {
      return Optional.empty();
    }
    return Optional.of(mProto.getScheme());
  }

  public Optional<Authority> getAuthority() {
    if (!mProto.hasAuthority()) {
      return Optional.empty();
    }
    return Optional.of(Authority.fromString(mProto.getAuthority()));
  }

  // TODO(Tony Sun): In the future Consider whether pathComponents should be extracted as a class.
  public List<String> getPathComponents() {
    return mProto.getPathComponentsList();
  }

  public UfsUrlMessage getProto() {
    return mProto;
  }

  public String asString() {
    // TODO(Jiacheng Liu): consider corner cases
    StringBuilder sb = new StringBuilder();
    sb.append(mProto.getScheme());
    if (!mProto.getScheme().isEmpty()) {
      sb.append(UfsUrl.SCHEME_SEPARATOR);
    }
    sb.append(mProto.getAuthority());
    if (!mProto.getAuthority().isEmpty()) {
      sb.append(UfsUrl.PATH_SEPARATOR);
    }
    List<String> pathComponents = mProto.getPathComponentsList();
    for (int i = 0; i < pathComponents.size(); i++) {
      if (pathComponents.get(i).isEmpty())  {
        continue;
      }
      sb.append(pathComponents.get(i));
      if (i != pathComponents.size() - 1) {
        sb.append(UfsUrl.PATH_SEPARATOR);
      }
    }
    return sb.toString();
  }

  public String asStringNoAuthority() {
    StringBuilder sb = new StringBuilder();
    sb.append(mProto.getScheme());
    sb.append("://");
    List<String> pathComponents = mProto.getPathComponentsList();
    for (int i = 0; i < pathComponents.size(); i++) {
      if (pathComponents.get(i).isEmpty())  {
        continue;
      }
      sb.append(pathComponents.get(i));
      if (i < pathComponents.size() - 1) {
        sb.append(AlluxioURI.SEPARATOR);
      }
    }
    return sb.toString();
  }

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

  // TODO(Jiacheng Liu): try to avoid the copy by a RelativeUrl class
  public UfsUrl getParentURL() {
    List<String> pathComponents = mProto.getPathComponentsList();
    return new UfsUrl(UfsUrlMessage.newBuilder()
        .setScheme(mProto.getScheme())
        .setAuthority(mProto.getAuthority())
        // TODO(Jiacheng Liu): how many copies are there. Improve the performance in the future.
        .addAllPathComponents(pathComponents.subList(0, pathComponents.size() - 1)).build());
  }

  // TODO(Jiacheng Liu): try to avoid the copy by a RelativeUrl class
  public UfsUrl getChildURL(String childName) {
    List<String> pathComponents = mProto.getPathComponentsList();
    return new UfsUrl(UfsUrlMessage.newBuilder()
        .setScheme(mProto.getScheme())
        .setAuthority(mProto.getAuthority())
        .addAllPathComponents(pathComponents).addPathComponents(childName).build());
  }

  public String getFullPath() {
    return Strings.join(mProto.getPathComponentsList(), PATH_SEPARATOR.charAt(0));
  }

  public AlluxioURI toAlluxioURI() {
    return new AlluxioURI(mProto.getScheme(),
        Authority.fromString(mProto.getAuthority()), getFullPath());
  }

  public int getDepth() {
    return getPathComponents().size();
  }

  public String getName() {
    List<String> pathComponents = getPathComponents();
    return pathComponents.get(pathComponents.size() - 1);
  }

  public boolean isAncestorOf(UfsUrl ufsUrl) throws InvalidPathException {
    if (!Objects.equals(getAuthority(), ufsUrl.getAuthority())) {
      return false;
    }
    if (!Objects.equals(getScheme(), ufsUrl.getScheme())) {
      return false;
    }
    return PathUtils.hasPrefix(PathUtils.normalizePath(ufsUrl.getFullPath(), PATH_SEPARATOR),
        PathUtils.normalizePath(getFullPath(), PATH_SEPARATOR));
  }

  public boolean isAbsolute() {
    return getScheme() != null;
  }

  public UfsUrl join(String suffix) {
    if (suffix.isEmpty()) {
      return this;
    }
    String[] suffixArray = suffix.split("/");
    int nonEmptyIndex = 0;
    while (nonEmptyIndex < suffixArray.length && suffixArray[nonEmptyIndex].isEmpty())  {
      nonEmptyIndex++;
    }
    List<String> suffixComponentsList = Arrays.asList(
        Arrays.copyOfRange(
            suffixArray,
            nonEmptyIndex, suffixArray.length));
    List<String> pathComponents = mProto.getPathComponentsList();
    return new UfsUrl(UfsUrlMessage.newBuilder()
        .setScheme(mProto.getScheme())
        .setAuthority(mProto.getAuthority())
        .addAllPathComponents(pathComponents)
        .addAllPathComponents(suffixComponentsList).build());
  }
}
