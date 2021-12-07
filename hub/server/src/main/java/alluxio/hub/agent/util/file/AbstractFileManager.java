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

package alluxio.hub.agent.util.file;

import alluxio.exception.AlluxioException;
import alluxio.security.authorization.Mode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.Arrays;
import java.util.Set;

/**
 * Abstract file manager.
 */
public abstract class AbstractFileManager implements  FileManager {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractFileManager.class);

  protected final File mRootDir;
  protected final String mUser;
  protected final String mGroup;

  /**
   * Construct a file manager from a string root.
   *
   * @param rootDir root directory
   * @param user User name
   * @param group Group Name
   */
  public AbstractFileManager(String rootDir, String user, String group) {
    this(new File(rootDir), user, group);
  }

  /**
   * Construct a file manager.
   *
   * @param rootDir root directory
   * @param user User name
   * @param group Group Name
   */
  private AbstractFileManager(File rootDir, String user, String group) {
    mRootDir = rootDir;
    mUser = user;
    mGroup = group;
    rootDir.mkdirs();
  }

  protected abstract String getNextFilePath(String fileName);

  @Override
  public boolean addFile(String fileName, String permission, byte[] content) {
    try {
      verifyFileName(fileName);
      Path path = Paths.get(getNextFilePath(fileName));
      short perm = Short.parseShort(permission, 8);
      Mode mode = new Mode(perm);
      Set<PosixFilePermission> permissions = PosixFilePermissions.fromString(mode.toString());
      FileAttribute<?> fileAttribute = PosixFilePermissions.asFileAttribute(permissions);
      Files.deleteIfExists(path);
      path = Files.createFile(path, fileAttribute);
      FileSystem fileSystem = path.getFileSystem();
      UserPrincipalLookupService service = fileSystem.getUserPrincipalLookupService();
      UserPrincipal userPrincipal = service.lookupPrincipalByName(mUser);
      GroupPrincipal groupPrincipal = service.lookupPrincipalByGroupName(mGroup);
      Files.write(path, content);
      Files.setOwner(path, userPrincipal);
      Files.getFileAttributeView(path, PosixFileAttributeView.class,
          LinkOption.NOFOLLOW_LINKS).setGroup(groupPrincipal);
      // sometimes umask is applied, so forcefully set permissions
      Files.setPosixFilePermissions(path, permissions);
      return true;
    } catch (InvalidPathException | IOException | AlluxioException e) {
      LOG.warn("Failed to add file {} to version manager", fileName, e);
      return false;
    }
  }

  private static final CharSequence[] INV_FILENAME = {"/", ".."};

  /**
   * Verifies that a filename conforms to a set of restrictions.
   *
   *
   * @param fname the filename that should be used
   * @throws AlluxioException if the filename is invalid
   */
  protected static void verifyFileName(String fname) throws AlluxioException  {
    if (fname.equals("")) {
      throw new AlluxioException("Filename may not be empty string");
    }
    PrestoCatalogUtils.validateAssetName(fname, "filename", Arrays.asList(INV_FILENAME));
  }
}
