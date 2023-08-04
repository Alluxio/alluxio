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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class UfsUrl {

  public static final String SCHEME_SEPARATOR = "://";
  public static final String PATH_SEPARATOR = "/";
  public static final String PORT_SEPARATOR = ":";

  UfsUrlMessage mProto;

  public static UfsUrlMessage toProto(String ufsPath) {
    Preconditions.checkArgument(ufsPath != null && !ufsPath.isEmpty(),
        "ufsPath is null or empty");
    String scheme = null;
    String authority = null;
    String path = null;

    int start = 0;
    int schemeSplitIndex = ufsPath.indexOf(SCHEME_SEPARATOR, start);
    if (schemeSplitIndex == -1) {
      scheme = "";
    } else {
      scheme = ufsPath.substring(start, schemeSplitIndex);
    }
    start += scheme.length() + SCHEME_SEPARATOR.length();

    int authSplitIndex = ufsPath.indexOf(PATH_SEPARATOR, start);
    if (authSplitIndex == -1) {
      authority = ufsPath.substring(start);
    } else {
      authority = ufsPath.substring(start, authSplitIndex);
    }
    start += authority.length();
    path = ufsPath.substring(start);

    List<String> pathComponents = Arrays.asList(path.split(PATH_SEPARATOR));
    return UfsUrlMessage.newBuilder()
        .setScheme(scheme)
        .setAuthority(authority)
        .addAllPathComponents(pathComponents).build();
  }

  public static UfsUrl createInstance(UfsUrlMessage proto) {
    return new UfsUrl(proto);
  }

  public static UfsUrl createInstance(String ufsPath) {
    Preconditions.checkArgument(ufsPath != null && !ufsPath.isEmpty(),
        "input path is null or empty");
    String ufsRootDir = Configuration.getString(PropertyKey.DORA_CLIENT_UFS_ROOT);
    Preconditions.checkArgument(ufsRootDir != null && !ufsRootDir.isEmpty(),
        "root dir is null or empty.");
    UfsUrlMessage rootMessage = toProto(ufsRootDir);
    UfsUrlMessage ufsUrlMessage = toProto(ufsPath);

    String scheme = null;
    String authority = null;
    List<String> pathComponents = new ArrayList<String>();

    // parse scheme
    if (rootMessage.getScheme().isEmpty())  {
      if (ufsUrlMessage.getScheme().isEmpty())  {
        scheme = "file";
      } else {
        scheme = ufsUrlMessage.getScheme();
      }
    } else {
      if (ufsUrlMessage.getScheme().isEmpty())  {
        scheme = rootMessage.getScheme();
      } else {
        scheme = ufsUrlMessage.getScheme();
      }
    }

    // parse authority
    if (rootMessage.getAuthority().isEmpty())  {
      authority = ufsUrlMessage.getAuthority();
    } else {
      if (rootMessage.getScheme().equals(ufsUrlMessage.getScheme())) {
        if (!ufsUrlMessage.getAuthority().isEmpty()) {
          authority = ufsUrlMessage.getAuthority();
        } else {
          authority = rootMessage.getAuthority();
        }
      } else {
        authority = ufsUrlMessage.getAuthority();
      }
    }

    // parse path
    if (scheme.equals("file") || rootMessage.getScheme().equals(ufsUrlMessage.getScheme())) {
      pathComponents.addAll(rootMessage.getPathComponentsList());
      // If two schemes are not equal,
      // it is possible to add root path only when ufsUrl has an empty scheme.
    } else if (ufsUrlMessage.getScheme().isEmpty()) {
      if (rootMessage.getAuthority().equals(ufsUrlMessage.getAuthority()))  {
        pathComponents.addAll(rootMessage.getPathComponentsList());
        // If two authorities are not equal,
        // it is possible to add root path when ufsUrl has an empty authority
      } else if (ufsUrlMessage.getAuthority().isEmpty()) {
        pathComponents.addAll(rootMessage.getPathComponentsList());
      }
    }

    pathComponents.addAll(ufsUrlMessage.getPathComponentsList());

    return new UfsUrl(UfsUrlMessage.newBuilder()
        .setScheme(scheme)
        .setAuthority(authority)
        .addAllPathComponents(pathComponents)
        .build());
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
    Preconditions.checkArgument(!pathComponents.isEmpty());
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

  public UfsUrl join(String suffix) {
    if (suffix == null || suffix.isEmpty()) {
      return new UfsUrl(mProto);
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
