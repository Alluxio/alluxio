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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UfsUrl {
  UfsUrlMessage mProto;

  public UfsUrl(UfsUrlMessage proto) {
    mProto = proto;
  }

  public UfsUrl(String ufsPath) {
    // TODO(Tony Sun): Consider corner cases carefully.
    //  1. more than one ':'
    //  2.

    ArrayList<String> preprocessingPathList = new ArrayList<>(Arrays.asList(ufsPath.split(":")));
    // TODO(Tony Sun): verify the correctness.
    Preconditions.checkArgument(preprocessingPathList.size() == 2,
            "Please ensure the ufsPath has only one ':'.");
    Preconditions.checkArgument(preprocessingPathList.get(1).startsWith("//"),
            "Please input a valid path like 'scheme://path'.");
    // TODO(Tony Sun): Will an input path contain authority?
    String scheme = preprocessingPathList.get(0);
    String realPath = preprocessingPathList.get(1).substring(2);

    // TODO(Tony Sun): rethink the errorMessage here.
    Preconditions.checkArgument(!realPath.isEmpty(), "Please input a not empty path.");

    // TODO(Tony Sun): What is an valid realPath format? Only containing '/', '_', digit and uppercase and lowercase?

    // TODO(Tony Sun): Do we need to handle realPath like '/////path/to/dir' ?"
    ArrayList<String> pathComponentsList = new ArrayList<String>(Arrays.asList(realPath.split("/")));


    // TODO(Tony Sun): Add scheme judgement, eg. limit the scheme type.

    mProto = UfsUrlMessage.newBuilder()
            .setScheme(scheme)
            .addAllPathComponents(pathComponentsList)
            .build();
    // 1. scheme not exist.
    // 2. how to determine the type of path.
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
    // TODO: consider corner cases
    StringBuilder sb = new StringBuilder();
    sb.append(mProto.getScheme());
    sb.append("://");
    sb.append(mProto.getAuthority());
    sb.append(AlluxioURI.SEPARATOR);
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
    // TODO: implement this

    return false;
  }

  public boolean equals(Object o) {
    // TODO: implement this
    return false;
  }

  // TODO: try to avoid the copy by a RelativeUrl class
  public UfsUrl getParentURL() {
    List<String> pathComponents = mProto.getPathComponentsList();
    return new UfsUrl(UfsUrlMessage.newBuilder()
            .setScheme(mProto.getScheme())
            .setAuthority(mProto.getAuthority())
            // TODO: how many copies are there
            .addAllPathComponents(pathComponents.subList(0, pathComponents.size() - 1)).build());
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
    return new AlluxioURI(mProto.getScheme(), Authority.fromString(mProto.getAuthority()), getFullPath());
  }
}
