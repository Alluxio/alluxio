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

package alluxio.worker.s3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * S3 Netty Tasks to handle bucket level or global level request.
 * (only bucket name or no bucket name is provided)
 */
public class S3NettyBucketTask extends S3NettyBaseTask {
  private static final Logger LOG = LoggerFactory.getLogger(S3NettyBucketTask.class);

  /**
   * Constructs an instance of {@link S3NettyBucketTask}.
   * @param handler
   * @param OPType
   */
  protected S3NettyBucketTask(S3NettyHandler handler, OpType OPType) {
    super(handler, OPType);
  }

  @Override
  public void continueTask() {
  }
}
