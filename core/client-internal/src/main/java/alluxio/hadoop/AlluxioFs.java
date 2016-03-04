/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.hadoop;

import alluxio.Constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * The AlluxioFs implementation of Hadoop AbstractFileSystem. The implementation delegates to the
 * existing Alluxio {@link alluxio.hadoop.FileSystem} and is only necessary for use with
 * Hadoop 2.x. Configuration example in Hadoop core-site.xml file:
 * <pre>
 * &lt;property&gt;
 *    &lt;name>fs.AbstractFileSystem.alluxio.impl&lt;/name&gt;
 *    &lt;value>alluxio.hadoop.AlluxioFs&lt;/value&gt;
 * &lt;/property&gt;
 * </pre>
 *
 * For long term solution, we need to rewrite AlluxioFs by extending Hadoop
 * {@link org.apache.hadoop.fs.AbstractFileSystem} directly.
 */
public class AlluxioFs extends DelegateToFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * This constructor has the signature needed by
   * {@link org.apache.hadoop.fs.AbstractFileSystem#createFileSystem(URI, Configuration)}
   * in Hadoop 2.x.
   *
   * @param uri the uri for this AlluxioFs filesystem
   * @param conf Hadoop configuration
   * @throws java.io.IOException if an I/O error occurs
   * @throws java.net.URISyntaxException if <code>uri</code> has syntax error
   */
  AlluxioFs(final URI uri, final Configuration conf) throws IOException, URISyntaxException {
    super(uri, new FileSystem(), conf, Constants.SCHEME, false);
  }

  @Override
  public int getUriDefaultPort() {
    return Constants.DEFAULT_MASTER_PORT;
  }
}
