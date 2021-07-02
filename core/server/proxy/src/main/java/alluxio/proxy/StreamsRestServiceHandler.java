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

package alluxio.proxy;

import alluxio.RestUtils;
import alluxio.StreamCache;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.conf.ServerConfiguration;
import alluxio.web.ProxyWebServer;

import com.google.common.io.ByteStreams;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;
import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * This class is a REST handler for stream resources.
 */
@NotThreadSafe
@Api(value = "/streams", description = "RESTful gateway for Alluxio Filesystem Client (Data)")
@Path(StreamsRestServiceHandler.SERVICE_PREFIX)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public final class StreamsRestServiceHandler {
  public static final String SERVICE_PREFIX = "streams";

  public static final String ID_PARAM = "{id}/";

  public static final String CLOSE = "close";
  public static final String READ = "read";
  public static final String WRITE = "write";

  private final StreamCache mStreamCache;

  /**
   * Constructs a new {@link StreamsRestServiceHandler}.
   *
   * @param context context for the servlet
   */
  public StreamsRestServiceHandler(@Context ServletContext context) {
    mStreamCache =
        (StreamCache) context.getAttribute(ProxyWebServer.STREAM_CACHE_SERVLET_RESOURCE_KEY);
  }

  /**
   * @summary closes a stream
   * @param id the stream id
   * @return the response object
   */
  @POST
  @Path(ID_PARAM + CLOSE)
  @ApiOperation(value = "Closes the stream associated with the id", response = java.lang.Void.class)
  public Response close(@PathParam("id") final Integer id) {
    return RestUtils.call((RestUtils.RestCallable<Void>) () -> {
      // When a stream is invalidated from the cache, the removal listener of the cache will
      // automatically close the stream.
      if (mStreamCache.invalidate(id) == null) {
        throw new IllegalArgumentException("stream does not exist");
      }
      return null;
    }, ServerConfiguration.global());
  }

  /**
   * @summary reads from a stream
   * @param id the stream id
   * @return the response object
   */
  @POST
  @Path(ID_PARAM + READ)
  @ApiOperation(value = "Returns the input stream associated with the id",
      response = java.io.InputStream.class)
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  public Response read(@PathParam("id") final Integer id) {
    // TODO(jiri): Support reading a file range.
    return RestUtils.call(new RestUtils.RestCallable<InputStream>() {
      @Override
      public InputStream call() throws Exception {
        FileInStream is = mStreamCache.getInStream(id);
        if (is != null) {
          return is;
        }
        throw new IllegalArgumentException("stream does not exist");
      }
    }, ServerConfiguration.global());
  }

  /**
   * @summary writes to a stream
   * @param id the stream id
   * @param is the input stream
   * @return the response object
   */
  @POST
  @Path(ID_PARAM + WRITE)
  @ApiOperation(value = "Writes to the given output stream associated with the id",
      response = java.lang.Integer.class)
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  public Response write(@PathParam("id") final Integer id, final InputStream is) {
    return RestUtils.call(() -> {
      FileOutStream os = mStreamCache.getOutStream(id);
      if (os != null) {
        return ByteStreams.copy(is, os);
      }
      throw new IllegalArgumentException("stream does not exist");
    }, ServerConfiguration.global());
  }
}
