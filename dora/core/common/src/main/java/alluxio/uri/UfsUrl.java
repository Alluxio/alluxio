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
import alluxio.grpc.UfsUrlMessage;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.util.Strings;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class UfsUrl {

  // TODO(Tony Sun): replace each xx_separator by the static variables.
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
    //  when scheme does not exist, how to determine the scheme, or a empty scheme.
    Preconditions.checkArgument(!ufsPath.isEmpty(),
            "ufsPath is empty, please input a non-empty ufsPath.");
    List<String> preprocessingPathList = Arrays.asList(ufsPath.split(SCHEME_SEPERATOR));

    // TODO(Tony Sun): Does every input path contain authority?
    String scheme = preprocessingPathList.get(0);
    String authorityAndPath = null;
    // If ufsPath has no '://', then the preprocessingPathList has only one elem.
    // TODO(Tony Sun): Refactor it later. Now default ufs is local, the design is ugly.
    if (preprocessingPathList.size() == 1)  {
      scheme = "file";
      authorityAndPath = preprocessingPathList.get(0);
    } else if (preprocessingPathList.size() == 2) {
      authorityAndPath = preprocessingPathList.get(1);
    }
    // TODO(Tony Sun): what if preprocessingPathList.size() > 2? Fix it!
    int indexOfFirstSlashAfterAuthority = authorityAndPath.indexOf(PATH_SEPERATOR);
    // Empty path is excluded here.
    Preconditions.checkArgument(indexOfFirstSlashAfterAuthority != -1,
        "Please input a valid path.");
    String authorityString = authorityAndPath.substring(0, indexOfFirstSlashAfterAuthority);
    String pathString = authorityAndPath.substring(indexOfFirstSlashAfterAuthority);
    String rootDir = Configuration.getString(PropertyKey.DORA_CLIENT_UFS_ROOT);
    if (scheme.equals("file")) {
      pathString = rootDir + pathString;
    }
    // TODO(Tony Sun): Do we need to handle path like '/////path/to/dir'?
    //  eg. remove the empty String.
    String[] arrayOfPathString = pathString.split(PATH_SEPERATOR);
    int indexOfNonEmptyString = 0;
    while (indexOfNonEmptyString < arrayOfPathString.length
        && arrayOfPathString[indexOfNonEmptyString].isEmpty()) {
      indexOfNonEmptyString += 1;
    }
    List<String> pathComponentsList = Arrays.asList(
        Arrays.copyOfRange(arrayOfPathString, indexOfNonEmptyString, arrayOfPathString.length));
    // TODO(Tony Sun): Add scheme judgement, eg. limit the scheme type.
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
    // TODO(Tony Sun): Why not override.
    // TODO: consider corner cases
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
      // TODO: need a trailing separator if the path is dir?
    }
    return sb.toString();
  }

  public String asStringNoAuthority() {
    // TODO: consider corner cases
    StringBuilder sb = new StringBuilder();
    sb.append(mProto.getScheme());
    sb.append("://");
    List<String> pathComponents = mProto.getPathComponentsList();
    for (int i = 0; i < pathComponents.size(); i++) {
      sb.append(pathComponents.get(i));
      if (i < pathComponents.size() - 1) {
        sb.append(AlluxioURI.SEPARATOR);
      }
      // TODO: need a trailing separator if the path is dir?
    }
    return sb.toString();
  }

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

  // TODO: try to avoid the copy by a RelativeUrl class
  public UfsUrl getParentURL() {
    List<String> pathComponents = mProto.getPathComponentsList();
    return new UfsUrl(UfsUrlMessage.newBuilder()
            .setScheme(mProto.getScheme())
            .setAuthority(mProto.getAuthority())
            // TODO: how many copies are there
            .addAllPathComponents(pathComponents.subList(0, pathComponents.size() - 1)).build());
            // TODO(Tony Sun): performance consideration.
  }

  // TODO: try to avoid the copy by a RelativeUrl class
  public UfsUrl getChildURL(String childName) {
    List<String> pathComponents = mProto.getPathComponentsList();
    return new UfsUrl(UfsUrlMessage.newBuilder()
            .setScheme(mProto.getScheme())
            .setAuthority(mProto.getAuthority())
            .addAllPathComponents(pathComponents).addPathComponents(childName).build());
  }

  public String getFullPath() {
    // TODO(Tony Sun): Is it correct to return a '/' with empty pathComponents?
    return "/" + Strings.join(mProto.getPathComponentsList(), AlluxioURI.SEPARATOR.charAt(0));
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

  //TODO(Tony Sun): Add isAncestorOf() method.

  public boolean isRoot() {
    // TODO(Tony Sun): In AlluxioURI here remains judging hasAuthority(), why?
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
