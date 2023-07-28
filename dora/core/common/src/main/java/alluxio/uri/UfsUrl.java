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
    // TODO(Tony Sun): first check the path is not empty? Or when specific method is called?
    Preconditions.checkArgument(proto.getPathComponentsList().size() != 0,
        "the proto.path is empty, please check the proto first");
    mProto = proto;
  }

  public UfsUrl(String ufsPath) {
    // TODO(Tony Sun): Considering the case below:
    //  when scheme does not exist, how to determine the scheme, or a empty scheme.

    List<String> preprocessingPathList = Arrays.asList(ufsPath.split(SCHEME_SEPERATOR));
    // TODO(Tony Sun): verify the correctness.
    Preconditions.checkArgument(preprocessingPathList.size() == 2,
            "Please ensure the ufsPath has type like 'xx://xxx'.");

    // ufsurl: xx://127.0.0.1:6789/path/to/dir/file

    // TODO(Tony Sun): Does every input path contain authority?
    String scheme = preprocessingPathList.get(0);
    String authorityAndPath = preprocessingPathList.get(1);

    int endOfAuthority = authorityAndPath.indexOf(PATH_SEPERATOR);
    Preconditions.checkArgument(endOfAuthority != -1, "Please input a valid path.");

    String authorityString = authorityAndPath.substring(0, endOfAuthority);
    String pathString = authorityAndPath.substring(endOfAuthority);

    // TODO(Tony Sun): Is the input of UfsUrl the same as the input of AlluxioUri? Can I use
    //  the string test format like AlluxioURITest.java?

    // TODO(Tony Sun): Do we need to handle path like '/////path/to/dir'?
    //  eg. remove the empty String."
    List<String> pathComponentsList = Arrays.asList(pathString.split(PATH_SEPERATOR));
    Preconditions.checkArgument(pathComponentsList.size() > 0,
        "the path is empty, please check the proto first.");

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

}
