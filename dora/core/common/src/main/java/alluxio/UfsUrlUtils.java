package alluxio;

import alluxio.grpc.MountPointInfo;
import alluxio.grpc.UfsUrlMessage;
import alluxio.uri.Authority;
import alluxio.uri.URI;

import alluxio.uri.UfsUrl;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.util.Strings;

import java.util.Arrays;
import java.util.List;

/**
 *
 * TODO: implement the equivalence of URI
 * TODO: implement the equivalence of PathUtils
 * TODO: implement the equivalence of AlluixoURI
 * TODO: move/rename the UTs of above, to make sure we have guaranteed the same functionalities
 */
// TODO: move this to Util package
public class UfsUrlUtils {
  // TODO(jiacheng): this should be implemented elsewhere, where the MountTable is accessible
  public static MountPointInfo findMountPoint(UfsUrl ufsPath) {
    return null;
  }


  /**
   * Creates a {@link URI} from a string.
   *
   * @param uriStr URI string to create the {@link URI} from
   * @return the created {@link URI}
   */
  public static UfsUrl create(String uriStr) {
    Preconditions.checkArgument(uriStr != null, "Can not create a uri with a null path.");

    // TODO: throw error if it is windows format
    // add a slash in front of paths with Windows drive letters
//    if (AlluxioURI.hasWindowsDrive(uriStr, false)) {
//      uriStr = "/" + uriStr;
//    }

    // parse uri components
    String scheme = null;
    String authority = null;

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
      // TODO: sanity check for the string
      authority = uriStr.substring(start + 2, authEnd);
      start = authEnd;
    }

    // uri path is the rest of the string -- fragment not supported
    String path = uriStr.substring(start, uriStr.length());

    // TODO: we do not support a query format like path?id=abc, do we want to throw an error?
    //  A ? check can be slow
//    // Parse the query part.
//    int question = path.indexOf('?');
//    if (question != -1) {
//      // There is a query.
//      query = path.substring(question + 1);
//      path = path.substring(0, question);
//    }
    return create(scheme, authority, path);
  }

  /**
   * Creates a {@link UfsUrlUtils} from components.
   *
   * @param scheme the scheme string of the URI
   * @param authority the authority of the URI
   * @param path the path component of the URI
   * @return the created {@link URI}
   */
  public static UfsUrl create(String scheme, Authority authority, String path) {
    Preconditions.checkArgument(path != null, "Can not create a uri with a null path.");
    Preconditions.checkArgument(scheme != null && !scheme.isEmpty(), "Scheme is empty");
    return new UfsUrl(UfsUrlMessage.newBuilder().setScheme(scheme).setAuthority(authority.toString())
        .addAllPathComponents(Arrays.asList(path.split(AlluxioURI.SEPARATOR))).build());
  }

  public static UfsUrl create(String scheme, String authorityStr, String path) {
    Preconditions.checkArgument(path != null, "Can not create a uri with a null path.");
    Preconditions.checkArgument(scheme != null && !scheme.isEmpty(), "Scheme is empty");
    return new UfsUrl(UfsUrlMessage.newBuilder().setScheme(scheme).setAuthority(authorityStr)
            .addAllPathComponents(Arrays.asList(path.split(AlluxioURI.SEPARATOR))).build());
  }

  /**
   * Resolves a child {@link URI} against a parent {@link URI}.
   *
   * @param parent the parent
   * @param childPathComponents the child
   * @return the created {@link URI}
   */
  public static UfsUrl create(UfsUrl parent, String[] childPathComponents) {
    if (childPathComponents == null || childPathComponents.length == 0) {
      return parent;
    }
    return new UfsUrl(UfsUrlMessage.newBuilder().setScheme(parent.getScheme()).setAuthority(parent.getAuthorityString())
            .addAllPathComponents(parent.getPathComponents())
            .addAllPathComponents(Arrays.asList(childPathComponents)).build());
  }

  /**
   *
   * @return a new URI based off a URI and a new path component
   */
  public static UfsUrl create(UfsUrl parent, String childPath) {
    return create(parent, childPath.split(AlluxioURI.SEPARATOR));
  }
}
