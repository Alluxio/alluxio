package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.ListStatusPartialPOptions;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.metastore.ReadOnlyInodeStore;
import alluxio.resource.CloseableIterator;
import alluxio.util.io.PathUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

class ListStatusPartial {

  /**
   * If performing a partial listing, from an offset, this checks to ensure the offset
   * exists within the starting path.
   * @param path the listing path
   * @param context the listing context
   * @return the components of the path to the offset
   */
  static List<String> checkPartialListingOffset(
      InodeTree inodeTree,  AlluxioURI path, ListStatusContext context)
      throws FileDoesNotExistException, InvalidPathException {
    Optional<ListStatusPartialPOptions.Builder> pOptions = context.getPartialOptions();
    if (!pOptions.isPresent()) {
      return Collections.emptyList();
    }
    ListStatusPartialPOptions.Builder partialOptions = pOptions.get();
    List<String> partialPathNames = Collections.emptyList();
    if (context.isPartialListing() && partialOptions.getOffset() != 0) {
      try {
        // See if the inode from where to start the listing exists.
        partialPathNames = inodeTree.getPathInodeNames(partialOptions.getOffset());
      } catch (FileDoesNotExistException e) {
        throw new FileDoesNotExistException(
            ExceptionMessage.INODE_NOT_FOUND_PARTIAL_LISTING.getMessage(e.getMessage()),
            e.getCause());
      }
      String[] pathComponents = PathUtils.getPathComponents(path.getPath());
      // the offset path must be at least as long as the starting path
      if (partialPathNames.size() < pathComponents.length) {
        throw new FileDoesNotExistException(ExceptionMessage.INODE_NOT_FOUND_PARTIAL_LISTING
            .getMessage(path));
      }
      for (int i = 0; i < pathComponents.length; i++) {
        if (!partialPathNames.get(i).equals(pathComponents[i])) {
          throw new FileDoesNotExistException(ExceptionMessage.INODE_NOT_FOUND_PARTIAL_LISTING
              .getMessage(path));
        }
      }
    }
    return partialPathNames;
  }

  /**
   * If this is a partial listing, this will compute the path components from where to start
   * the listing from that come after the root listing path.
   *
   * @param context the context of the operation
   * @param pathNames the full path from where the partial listing is expected to start,
   *                  null if this is the first listing
   * @param rootPath the locked root path of the listing
   * @return the path components after the root path from where to start the partial listing,
   * or null if the listing should start from the beginning of the root path
   */
  static List<String> computePartialListingPaths(
      ListStatusContext context,
      List<String> pathNames, LockedInodePath rootPath)
      throws InvalidPathException, FileDoesNotExistException {

    if (!context.getOptions().getRecursive()) {
      if (rootPath.getInode().isDirectory()) {
        rootPath.getInode().asDirectory().getChildCount();
      }
    }
    if (pathNames.isEmpty()) {
      Optional<ListStatusPartialPOptions.Builder> partialOptions = context.getPartialOptions();
      if (partialOptions.isPresent()) {
        // use the startAfter option, since this is the first listing
        if (!partialOptions.get().getStartAfter().isEmpty()) {
          return Arrays.stream(PathUtils.getPathComponents(
                  partialOptions.get().getStartAfter()))
              .skip(1).collect(Collectors.toList());
        }
      }
      // otherwise, start from the beginning of the listing
      return Collections.emptyList();
    }
    // compute where to start from in each depth, we skip past the rootInodes, since that is
    // where we start the traversal from
    List<String> partialPath = pathNames.stream().skip(rootPath.size())
        .collect(Collectors.toList());
    if (partialPath.size() > 0) {
      return partialPath;
    }
    return Collections.emptyList();
  }

  /**
   * If listing using a prefix and a partial path, this will compute whether the prefix
   * exits in the partial path.
   *
   * @param partialPath the components of the path from where to start the listing
   * @return the components of the prefix split by the delimiter /
   * @throws InvalidPathException if the path in prefixComponents does not exist in partialPath
   */
  static List<String> checkPrefixListingPaths(
      ListStatusContext context, List<String> partialPath)
      throws InvalidPathException {
    Optional<ListStatusPartialPOptions.Builder> pOptions = context.getPartialOptions();
    if (!pOptions.isPresent()) {
      return Collections.emptyList();
    }
    List<String> prefixComponents;
    ListStatusPartialPOptions.Builder partialOptions = pOptions.get();
    if (!partialOptions.getPrefix().isEmpty()) {
      // compute the prefix as path components, removing the first empty string
      prefixComponents = Arrays.stream(PathUtils.getPathComponents(
              new AlluxioURI(
                  partialOptions.getPrefix()).getPath())).skip(1)
          .collect(Collectors.toList());
    } else {
      prefixComponents = Collections.emptyList();
    }
    // we only have to check the prefix if we are doing a partial listing,
    // and we are not on the initial partial listing call
    if (partialPath.isEmpty()
        || !(partialOptions.hasOffset() && partialOptions.getOffset() != 0)) {
      return prefixComponents;
    }
    // for each component the prefix must be the same as the partial path component
    // except at the last component where the prefix must be contained in the partial path component
    if (!PathUtils.hasPrefixComponentsCanBeLonger(partialPath, prefixComponents)) {
      throw new InvalidPathException(
          ExceptionMessage.PREFIX_DOES_NOT_MATCH_PATH
              .getMessage(prefixComponents, partialPath));
    }
    return prefixComponents;
  }

  /**
   * This will generate the iterator of children during a partial listing call
   * for the given depth and options.
   * @param inodeStore the inode store
   * @param inode the parent inode
   * @param partialPath the components of the path from where to start the listing
   * @param prefixComponents the components of the prefix to list
   * @param depth the depth of the children
   * @param context the list status context
   * @return the iterator of children
   */
  static CloseableIterator<? extends Inode> getChildrenIterator(
      ReadOnlyInodeStore inodeStore, Inode inode, List<String> partialPath,
      List<String> prefixComponents, int depth, ListStatusContext context)
      throws InvalidPathException {

    // Check if we should process all children, or just the partial listing, or just a prefix
    String prefix = null;
    if (prefixComponents.size() > depth) {
      prefix = prefixComponents.get(depth);
    }
    ListStatusPartialPOptions.Builder partialOptions = context.getPartialOptions().orElseThrow(
        () -> new RuntimeException("Method should only be called when doing partial listing"));
    if (partialOptions.hasOffset() || partialOptions.hasStartAfter()) {
      // If we have already processed the first entry in the partial path
      // then we just process from the start of the children, so we list from the empty string
      String listFrom = "";
      if (partialPath.size() > depth) {
        listFrom = partialPath.get(depth);
        if (prefix != null) {
          // the prefix must have the same components as the start point in the partial listing
          // because the node must have matched the prefix in the previous call to the listing
          if ((prefixComponents.size() > depth + 1 && !prefix.equals(listFrom))) {
            throw new InvalidPathException(ExceptionMessage.PREFIX_DOES_NOT_MATCH_PATH.getMessage(
                prefix, listFrom));
          }
        }
      }
      if (prefix != null) {
        return inodeStore.getChildrenPrefixFrom(inode.getId(), prefix, listFrom);
      } else {
        return inodeStore.getChildrenFrom(inode.getId(), listFrom);
      }
    } else if (prefix != null) {
      return inodeStore.getChildrenPrefix(inode.getId(), prefix);
    }
    // Perform a full listing of all children sorted by name.
    return inodeStore.getChildren(inode.asDirectory());
  }
}
