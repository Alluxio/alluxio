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

package alluxio.hadoop;

import alluxio.Constants;
import alluxio.conf.PropertyKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * The Alluxio implementation of Hadoop AbstractFileSystem. The implementation delegates to the
 * existing Alluxio {@link alluxio.hadoop.FileSystem} and is only necessary for use with
 * Hadoop 2.x. Configuration example in Hadoop core-site.xml file:
 * <pre>
 * &lt;property&gt;
 *    &lt;name>fs.AbstractFileSystem.alluxio.impl&lt;/name&gt;
 *    &lt;value>alluxio.hadoop.AlluxioFileSystem&lt;/value&gt;
 * &lt;/property&gt;
 * </pre>
 *
 * For a long term solution, we need to rewrite AlluxioFileSystem by extending Hadoop's
 * {@link AbstractFileSystem} directly.
 */
public class AlluxioFileSystem extends DelegateToFileSystem {
  /**
   * This constructor has the signature needed by
   * {@link AbstractFileSystem#createFileSystem(URI, Configuration)}
   * in Hadoop 2.x.
   *
   * @param uri the uri for this Alluxio filesystem
   * @param conf Hadoop configuration
   * @throws URISyntaxException if <code>uri</code> has syntax error
   */
  AlluxioFileSystem(final URI uri, final Configuration conf)
      throws IOException, URISyntaxException {
    super(uri, new FileSystem(), conf, Constants.SCHEME, false);
  }

  @Override
  public int getUriDefaultPort() {
    return Integer.parseInt(PropertyKey.MASTER_RPC_PORT.getDefaultValue());
  }
}
