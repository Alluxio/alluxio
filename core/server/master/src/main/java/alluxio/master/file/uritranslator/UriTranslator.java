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

package alluxio.master.file.uritranslator;

import alluxio.AlluxioURI;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.MountTable;
import alluxio.util.CommonUtils;

public interface UriTranslator {
  /**
   * Factory for {@link UriTranslator}.
   */
  class Factory {

    // prevent instantiation
    private Factory() {}

    /**
     * @param master Alluxio file system master
     * @param mountTable Alluxio mount table
     * @param inodeTree inode tree of the file system master
     * @return the generated {@link UriTranslator}
     */
    public static UriTranslator create(FileSystemMaster master, MountTable mountTable,
        InodeTree inodeTree) {
      String className =
          ServerConfiguration.get(PropertyKey.MASTER_URI_TRANSLATOR_IMPL);
      Class<?> providerClass;
      try {
        providerClass = Class.forName(className);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(className + " not found");
      }

      return (UriTranslator) CommonUtils
          .createNewClassInstance(providerClass,
              new Class[] {FileSystemMaster.class, MountTable.class, InodeTree.class},
              new Object[] {master, mountTable, inodeTree});
    }
  }

  /**
   * Translate the input uri to Alluxio uri.
   *
   * @param uriStr the input uri
   * @return alluxio uri
   * @throws InvalidPathException if the path is invalid
   */
  AlluxioURI translateUri(String uriStr) throws InvalidPathException;
}
