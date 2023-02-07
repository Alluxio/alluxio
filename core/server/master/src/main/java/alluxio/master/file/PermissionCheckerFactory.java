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

package alluxio.master.file;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.file.meta.InodeTree;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;

/**
 * Factory to create a {@link PermissionChecker} instance.
 */
public final class PermissionCheckerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(PermissionCheckerFactory.class);

  /**
   * Constructs a new {@link PermissionCheckerFactory}.
   */
  public PermissionCheckerFactory() {
  }

  /**
   * Create an object with class that implements the interface of PermissionChecker.
   * @param inodeTree Instance of InodeTree
   * @return return the object of PermissionChecker
   */
  public PermissionChecker create(InodeTree inodeTree) {
    String permissionClass =
        Configuration.getOrDefault(PropertyKey.PERMISSION_CHECKER_CLASS, null);

    if (permissionClass == null) {
      return new DefaultPermissionChecker(inodeTree);
    }

    LOG.info("User provided PermissionChecker class: %s is going to be loaded", permissionClass);
    return (PermissionChecker) loadClassByName(permissionClass, inodeTree);
  }

  private Object loadClassByName(String className, Object ... initArgs) throws RuntimeException {
    try {
      return ClassLoader.getSystemClassLoader()
              .loadClass(className)
              .getConstructor(InodeTree.class)
              .newInstance(initArgs);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(className + "is not found", e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Constructor is not found in class " + className, e);
    } catch (SecurityException | IllegalAccessException e) {
      throw new RuntimeException("There is security issue when invoking the class " + className, e);
    } catch (InvocationTargetException | InstantiationException e) {
      throw new RuntimeException("Exception happens when invoking the class " + className, e);
    }
  }
}
