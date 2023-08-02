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

  public UfsUrl(UfsUrlMessage proto) {
    Preconditions.checkArgument(proto.getPathComponentsList().size() != 0,
        "The proto.path is empty, please check the proto first");
    mProto = proto;
  }

  public UfsUrl(String ufsPath) {
    Preconditions.checkArgument(!ufsPath.isEmpty(),
        "ufsPath is empty, please input a non-empty ufsPath.");

    String rootDir = Configuration.getString(PropertyKey.DORA_CLIENT_UFS_ROOT);
    String rootScheme;
    String rootAuthority;
    String rootPath;
    String[] rootDirArray = rootDir.split(SCHEME_SEPARATOR);
    if (!ufsPath.equals(rootDir)) {
      // rootDir = "hdfs:///" -> rootDirArray = ["hdfs", "/"]
      Preconditions.checkArgument(rootDirArray.length <= 2, "Invalid rootDir, "
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

    List<String> preprocessingPathList = Arrays.asList(ufsPath.split(SCHEME_SEPARATOR));
    String scheme;
    String authority;
    String path;
    String authorityAndPath;
    Preconditions.checkArgument(preprocessingPathList.size() <= 2,
        "There are multiple schemes, the input may contain more than one path, "
            + "current version only support inputting one path each time.");
    // Now, there are two condition. size = 1 -> without scheme; size = 2 -> with a scheme.
    if (preprocessingPathList.size() == 1)  {
      // If without scheme, set default scheme to root.
      if (rootDirArray.length == 1) {
        // if length == 1, means rootDirArray is not include scheme.
        scheme = rootScheme;
      } else {
        scheme = rootDirArray[0];
      }
      authorityAndPath = preprocessingPathList.get(0);
    } else {
      // preprocessingPathList.size() == 2, i.e. the ufsPath has one scheme.
      scheme = preprocessingPathList.get(0);
      authorityAndPath = preprocessingPathList.get(1);
    }
    Preconditions.checkNotNull(scheme, "scheme is empty, please input again");
    Preconditions.checkNotNull(authorityAndPath, "authority or path is empty, please input again");

    int indexOfFirstColon = authorityAndPath.indexOf(PORT_SEPARATOR);
    if (indexOfFirstColon == -1)  {
      // There are no ":", i.e., no authority.
      authority = rootAuthority;
      // Handle case with two slash like "/tmp/" + "/cache" = "/tmp//cache"
      path = UfsUrlUtils.concatStringPath(rootPath, authorityAndPath);
    } else {
      // The string has authority.
      int indexOfFirstSlash = authorityAndPath.indexOf(PATH_SEPARATOR);
      authority = authorityAndPath.substring(0, indexOfFirstSlash);
      String tmpPath = authorityAndPath.substring(indexOfFirstSlash);
      path = UfsUrlUtils.concatStringPath(rootPath, tmpPath);
    }

//    int indexOfFirstSlashAfterAuthority = authorityAndPath.indexOf(PATH_SEPARATOR);
//    if (indexOfFirstSlashAfterAuthority == -1)  {
//      // If index is 0, authority or path is empty. hdfs://xxx:1234 or hdfs://dir
//      indexOfFirstSlashAfterAuthority = 0;
//    }
//
//    String authorityString = authorityAndPath.substring(0, indexOfFirstSlashAfterAuthority);
//    // path string is beginning with '/'.
//    String pathString = authorityAndPath.substring(indexOfFirstSlashAfterAuthority);
//
//    if (scheme.equalsIgnoreCase("file") || scheme.equalsIgnoreCase("alluxio")
//        || scheme.equalsIgnoreCase("s3") || scheme.equalsIgnoreCase("hdfs")) {
//      pathString = rootDir + pathString;
//    } else {
//      System.out.println("Other scheme are not supported currently.");
//      return;
//    }

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
