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

package alluxio.extension;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * Interface for a client in the Alluxio system.
 */
public class ExtensionClassLoader extends URLClassLoader {

  public ExtensionClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);
  }

  @Override
  protected Class<?> findClass(String name) throws ClassNotFoundException {
    if (!loadFromExtension(name)) {
      return super.findClass(name);
    }
    throw new ClassCastException("Unable to find class " + name + " in extension URL.");
  }

  protected boolean loadFromExtension(String name) {
    // TODO(adit): implement me
    return false;
  }

  protected String getPackageName(String className) {
    int idx = className.lastIndexOf('.');
    return idx >= 0 ? className.substring(0, idx) : null;
  }
}
