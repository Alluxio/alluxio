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

package alluxio.worker.http;

import io.netty.channel.FileRegion;
import io.netty.handler.codec.http.HttpResponse;

/**
 * Http response context for wrapping useful information.
 */
public class HttpResponseContext {

  private final HttpResponse mHttpResponse;

  private final FileRegion mFileRegion;

  /**
   * Http response context for wrapping useful information.
   * @param httpResponse the http response to client
   * @param fileRegion the file region to read from the worker side
   */
  public HttpResponseContext(HttpResponse httpResponse, FileRegion fileRegion) {
    mHttpResponse = httpResponse;
    mFileRegion = fileRegion;
  }

  /**
   * Get the http response.
   * @return the http response
   */
  public HttpResponse getHttpResponse() {
    return mHttpResponse;
  }

  /**
   * Get the file region to read from the worker side.
   * @return the file region
   */
  public FileRegion getFileRegion() {
    return mFileRegion;
  }
}
