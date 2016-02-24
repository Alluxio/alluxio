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
 * The AlluxioFS implementation of Hadoop AbstractFileSystem. The implementation delegates to the
 * existing Alluxio FileSystem and Only necessary for use with Hadoop 2.x.
 * Configuration example in Hadoop core-site.xml file:
 * <pre>
 * &lt;property&gt;
 *    &lt;name>fs.AbstractFileSystem.alluxio.impl&lt;/name&gt;
 *    &lt;value>alluxio.hadoop.AlluxioFs&lt;/value&gt;
 * &lt;/property&gt;
 * </pre>
 */
// TODO(pfxuan): DelegateToFileSystem is a private/unstable interface.
// For long term solution, we need to rewrite AlluxioFs by extending Hadoop
// AbstractFileSystem directly.
// https://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/AbstractFileSystem.html
public class AlluxioFs extends DelegateToFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * This constructor has the signature needed by
   * {@link org.apache.hadoop.fs.AbstractFileSystem#createFileSystem(java.net.URI, Configuration)}.
   *
   * @param uri which must be that of AlluxioFS
   * @param conf
   * @throws java.io.IOException
   * @throws java.net.URISyntaxException
   */
  AlluxioFs(final URI uri, final Configuration conf) throws IOException, URISyntaxException {
    super(uri, new FileSystem(), conf, Constants.SCHEME, false);
  }

  @Override
  public int getUriDefaultPort() {
    return Constants.DEFAULT_MASTER_PORT;
  }
}
