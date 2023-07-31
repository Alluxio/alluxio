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
import org.apache.logging.log4j.util.Strings;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class UfsUrl {

  public static final String SCHEME_SEPERATOR = "://";
  public static final String PATH_SEPERATOR = "/";
  UfsUrlMessage mProto;

  public UfsUrl(UfsUrlMessage proto) {
    Preconditions.checkArgument(proto.getPathComponentsList().size() != 0,
        "the proto.path is empty, please check the proto first");
    // TODO(Tony Sun): trans proto to absolute path.
    mProto = proto;
  }

  public UfsUrl(String ufsPath) {
    // TODO(Tony Sun): Considering the case below:
    //  when scheme does not exist, how to determine the scheme, or an empty scheme.
    Preconditions.checkArgument(!ufsPath.isEmpty(),
            "ufsPath is empty, please input a non-empty ufsPath.");
    List<String> preprocessingPathList = Arrays.asList(ufsPath.split(SCHEME_SEPERATOR));
    String scheme;
    String authorityAndPath;
    String rootDir = Configuration.getString(PropertyKey.DORA_CLIENT_UFS_ROOT);
    Preconditions.checkArgument(preprocessingPathList.size() <= 2,
        "There are multiple schemes, the input may contain more than one path, "
            + "current version only support one path each time");
    // Now, there are two condition. size = 1 -> without scheme; size = 2 -> with a scheme.
    if (preprocessingPathList.size() == 1)  {
      // If without scheme, set default scheme to local, i.e. "file".
      String[] rootDirArray = rootDir.split(SCHEME_SEPERATOR);
      if (rootDirArray.length == 1) {
        scheme = "file";
      } else {
        scheme = rootDirArray[0];
      }
      authorityAndPath = preprocessingPathList.get(0);
    } else {
      // preprocessingPathList.size() == 2, i.e. the ufsPath has one scheme.
      scheme = preprocessingPathList.get(0);
      authorityAndPath = preprocessingPathList.get(1);
    }
    Preconditions.checkArgument(scheme.equalsIgnoreCase("file")
        || scheme.equalsIgnoreCase("s3") || scheme.equalsIgnoreCase("hdfs"),
        "Now UfsUrl only support local, s3, and hdfs");
    int indexOfFirstSlashAfterAuthority = authorityAndPath.indexOf(PATH_SEPERATOR);
    // Empty path is excluded here.
//    Preconditions.checkArgument(indexOfFirstSlashAfterAuthority != -1,
//        "Please input a valid path.");
    if (indexOfFirstSlashAfterAuthority == -1)  {
      // If index is 0, the authorityString will be empty.
      indexOfFirstSlashAfterAuthority = 0;
    }
    String authorityString = authorityAndPath.substring(0, indexOfFirstSlashAfterAuthority);
    String pathString = authorityAndPath.substring(indexOfFirstSlashAfterAuthority);

    if (scheme.equals("file")) {
      pathString = rootDir + pathString;
    } else {
      // TODO(Tony Sun): parse the dir part of each path. like s3 or hdfs.
      return;
    }
    String[] arrayOfPathString = pathString.split(PATH_SEPERATOR);
    List<String> pathComponentsList = Arrays.asList(arrayOfPathString);
    mProto = UfsUrlMessage.newBuilder()
        .setScheme(scheme)
        .setAuthority(authorityString)
        .addAllPathComponents(pathComponentsList)
        .build();
  }

  public boolean hasScheme() {
    return mProto.hasScheme();
  }

  public String getScheme() {
    return mProto.getScheme();
  }

  public boolean hasAuthority() {
    return mProto.hasAuthority();
  }

  public Authority getAuthority() {
    return Authority.fromString(mProto.getAuthority());
  }

  public String getAuthorityString() {
    return mProto.getAuthority();
  }

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
    sb.append(UfsUrl.SCHEME_SEPERATOR);
    sb.append(mProto.getAuthority());
    sb.append(UfsUrl.PATH_SEPERATOR);
    List<String> pathComponents = mProto.getPathComponentsList();
    for (int i = 0; i < pathComponents.size(); i++) {
      sb.append(pathComponents.get(i));
      if (i < pathComponents.size() - 1) {
        sb.append(UfsUrl.PATH_SEPERATOR);
      }
      // TODO(Jiacheng Liu): need a trailing separator if the path is dir?
    }
    return sb.toString();
  }

  public String asStringNoAuthority() {
    StringBuilder sb = new StringBuilder();
    sb.append(mProto.getScheme());
    sb.append("://");
    List<String> pathComponents = mProto.getPathComponentsList();
    for (int i = 0; i < pathComponents.size(); i++) {
      sb.append(pathComponents.get(i));
      if (i < pathComponents.size() - 1) {
        sb.append(AlluxioURI.SEPARATOR);
      }
    }
    return sb.toString();
  }

  // TODO(Tony Sun): Does prefix containing scheme and auth? In the future, considering performance.
  public boolean isPrefix(UfsUrl another, boolean allowEquals) {
    String thisString = asString();
    String anotherString = another.asString();
    if (anotherString.startsWith(thisString)) {
      if (Objects.equals(anotherString, thisString)) {
        return allowEquals;
      }
      else {
        return true;
      }
    }
    return false;
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
    return Strings.join(mProto.getPathComponentsList(), AlluxioURI.SEPARATOR.charAt(0));
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
    return PathUtils.hasPrefix(PathUtils.normalizePath(ufsUrl.getFullPath(), PATH_SEPERATOR),
        PathUtils.normalizePath(getFullPath(), PATH_SEPERATOR));
  }

  public boolean isRoot() {
    String rootDir = Configuration.getString(PropertyKey.DORA_CLIENT_UFS_ROOT);
    Preconditions.checkArgument(!(rootDir.startsWith(getFullPath())
        && !rootDir.equals(getFullPath())), "Current UfsUrl is the parent of root ufs directory");
    return getFullPath().equals(PATH_SEPERATOR) || getFullPath().isEmpty();
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
