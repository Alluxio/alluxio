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
import alluxio.collections.Pair;

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.net.URISyntaxException;

/**
 * This interface represents a URI. This {@link URI} supports more than just strict
 * {@link java.net.URI}. Some examples:
 *   * Windows paths
 *     * C:\
 *     * D:\path\to\file
 *     * E:\path\to\skip\..\file
 *   * URI with multiple scheme components
 *     * scheme://host:123/path
 *     * scheme:part2//host:123/path
 *     * scheme:part2://host:123/path
 *     * scheme:part2:part3//host:123/path
 *     * scheme:part2:part3://host:123/path
 *
 * Currently, does not support fragment in the URI.
 */
public interface URI extends Comparable<URI>, Serializable {

  /**
   * Factory for {@link URI}.
   */
  class Factory {

    private Factory() {} // prevent instantiation

    /**
     * Creates a {@link URI} from a string.
     *
     * @param uriStr URI string to create the {@link URI} from
     * @return the created {@link URI}
     */
    public static URI create(String uriStr) {
      Preconditions.checkArgument(uriStr != null, "Can not create a uri with a null path.");

      // add a slash in front of paths with Windows drive letters
      if (AlluxioURI.hasWindowsDrive(uriStr, false)) {
        uriStr = "/" + uriStr;
      }

      // parse uri components
      String scheme = null;
      String authority = null;
      String query = null;

      int start = 0;

      // parse uri scheme, if any
      int colon = uriStr.indexOf(':');
      int slash = uriStr.indexOf('/');
      if ((colon != -1) && ((slash == -1) || (colon < slash))) { // has a scheme
        if (slash != -1) {
          // There is a slash. The scheme may have multiple parts, so the scheme is everything
          // before the slash.
          start = slash;

          // Ignore any trailing colons from the scheme.
          while (slash > 0 && uriStr.charAt(slash - 1) == ':') {
            slash--;
          }
          scheme = uriStr.substring(0, slash);
        } else {
          // There is no slash. The scheme is the component before the first colon.
          scheme = uriStr.substring(0, colon);
          start = colon + 1;
        }
      }

      // parse uri authority, if any
      if (uriStr.startsWith("//", start) && (uriStr.length() - start > 2)) { // has authority
        int nextSlash = uriStr.indexOf('/', start + 2);
        int authEnd = nextSlash > 0 ? nextSlash : uriStr.length();
        authority = uriStr.substring(start + 2, authEnd);
        start = authEnd;
      }

      // uri path is the rest of the string -- fragment not supported
      String path = uriStr.substring(start, uriStr.length());

      // Parse the query part.
      int question = path.indexOf('?');
      if (question != -1) {
        // There is a query.
        query = path.substring(question + 1);
        path = path.substring(0, question);
      }
      return create(scheme, Authority.fromString(authority), path, query);
    }

    /**
     * Creates a {@link URI} from components.
     *
     * @param scheme the scheme string of the URI
     * @param authority the authority of the URI
     * @param path the path component of the URI
     * @param query the query component of the URI
     * @return the created {@link URI}
     */
    public static URI create(String scheme, Authority authority, String path, String query) {
      Preconditions.checkArgument(path != null, "Can not create a uri with a null path.");

      // Handle schemes with two components.
      Pair<String, String> schemeComponents = getSchemeComponents(scheme);
      String schemePrefix = schemeComponents.getFirst();
      scheme = schemeComponents.getSecond();

      if (scheme == null || schemePrefix.isEmpty()) {
        return new StandardURI(scheme, authority, path, query);
      } else {
        return new MultiPartSchemeURI(schemePrefix, scheme, authority, path, query);
      }
    }

    /**
     * Resolves a child {@link URI} against a parent {@link URI}.
     *
     * @param parent the parent
     * @param child the child
     * @return the created {@link URI}
     */
    public static URI create(URI parent, URI child) {
      // Add a slash to parent's path so resolution is compatible with URI's
      String parentPath = parent.getPath();
      if (!parentPath.endsWith(AlluxioURI.SEPARATOR) && parentPath.length() > 0) {
        parentPath += AlluxioURI.SEPARATOR;
      }
      java.net.URI parentUri;
      java.net.URI childUri;
      Authority authority = child.getAuthority() instanceof NoAuthority
          ? parent.getAuthority() : child.getAuthority();
      try {
        // To be compatible with URI, must use the last component of the scheme.
        parentUri = new java.net.URI(getSchemeComponents(parent.getScheme()).getSecond(),
            null, parentPath, parent.getQuery(), null);
        childUri = new java.net.URI(getSchemeComponents(child.getScheme()).getSecond(),
            null, child.getPath(), child.getQuery(), null);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e);
      }
      java.net.URI resolved = parentUri.resolve(childUri);

      String resolvedScheme;
      // Determine which scheme prefix to take.
      if (child.getPath() == null || parent.getPath() == null || child.getScheme() != null) {
        // With these conditions, the resolved URI uses the child's scheme.
        resolvedScheme = child.getScheme();
      } else {
        // Otherwise, the parent scheme is used in the resolved URI.
        resolvedScheme = parent.getScheme();
      }

      return create(resolvedScheme, authority, resolved.getPath(), resolved.getQuery());
    }

    /**
     * @param baseUri the base URI
     * @param newPath the new path component
     * @param checkNormalization if true, will check if the path requires normalization
     * @return a new URI based off a URI and a new path component
     */
    public static URI create(URI baseUri, String newPath, boolean checkNormalization) {
      Preconditions.checkArgument(newPath != null, "Can not create a uri with a null newPath.");
      return baseUri.createNewPath(newPath, checkNormalization);
    }

    /**
     * Returns a {@link Pair} of components of the given scheme. A given scheme may have have two
     * components if it has the ':' character to specify a sub-protocol of the scheme. If the
     * scheme does not have multiple components, the first component will be the empty string, and
     * the second component will be the given scheme. If the given scheme is null, both components
     * in the {@link Pair} will be null.
     *
     * @param scheme the scheme string
     * @return a {@link Pair} with the scheme components
     */
    public static Pair<String, String> getSchemeComponents(String scheme) {
      if (scheme == null) {
        return new Pair<>(null, null);
      }
      int colon = scheme.lastIndexOf(':');
      if (colon == -1) {
        return new Pair<>("", scheme);
      }
      return new Pair<>(scheme.substring(0, colon), scheme.substring(colon + 1));
    }
  }

  /**
   * @param newPath the new path component
   * @param checkNormalization if true, will check if the path requires normalization
   * @return a new URI based off of this URI, but with a new path component
   */
  URI createNewPath(String newPath, boolean checkNormalization);

  /**
   * @return the authority of the {@link URI}, null if it does not have one
   */
  Authority getAuthority();

  /**
   * @return the path of the {@link URI}
   */
  String getPath();

  /**
   * @return the query component of the {@link URI}
   */
  String getQuery();

  /**
   * @return the scheme of the {@link URI}, null if there is no scheme
   */
  String getScheme();

  /**
   * @return the scheme specific part of the {@link URI}, null if there is no scheme
   */
  String getSchemeSpecificPart();

  /**
   * Tells whether or not the {@link URI} is absolute.
   *
   * <p>
   * An {@link URI} is absolute if, and only if, it has a scheme component.
   * </p>
   *
   * @return <tt>true</tt> if, and only if, this {@link URI} is absolute
   */
  boolean isAbsolute();
}
