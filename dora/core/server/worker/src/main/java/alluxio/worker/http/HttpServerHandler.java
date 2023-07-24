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

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpHeaderValues.CLOSE;
import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;
import static io.netty.handler.codec.http.HttpHeaderValues.TEXT_PLAIN;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.exception.AlluxioException;
import alluxio.exception.PageNotFoundException;
import alluxio.grpc.ListStatusPOptions;
import alluxio.util.FileSystemOptionsUtils;

import com.google.gson.Gson;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link HttpServerHandler} deals with HTTP requests received from Netty Channel.
 */
public class HttpServerHandler extends SimpleChannelInboundHandler<HttpObject> {

  private static final Logger LOG = LoggerFactory.getLogger(HttpServerHandler.class);

  private final PagedService mPagedService;

  /**
   * {@link HttpServerHandler} deals with HTTP requests received from Netty Channel.
   * @param pagedService the {@link PagedService} object provides page related RESTful API
   */
  public HttpServerHandler(PagedService pagedService) {
    mPagedService = pagedService;
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws PageNotFoundException {
    if (msg instanceof HttpRequest) {
      HttpRequest req = (HttpRequest) msg;
      HttpResponseContext responseContext = dispatch(req);
      HttpResponse response = responseContext.getHttpResponse();

      boolean keepAlive = HttpUtil.isKeepAlive(req);
      if (keepAlive) {
        if (!req.protocolVersion().isKeepAliveDefault()) {
          response.headers().set(CONNECTION, KEEP_ALIVE);
        }
      } else {
        // Tell the client we're going to close the connection.
        response.headers().set(CONNECTION, CLOSE);
      }

      ChannelFuture channelFuture;
      if (response instanceof FullHttpResponse) {
        channelFuture = ctx.write(response);
      } else {
        ctx.write(response);
        ctx.write(responseContext.getFileRegion());
        channelFuture = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
      }

      if (!keepAlive) {
        channelFuture.addListener(ChannelFutureListener.CLOSE);
      }
    }
  }

  private Map<String, String> parseRequestParameters(String requestUri) {
    requestUri = requestUri.substring(requestUri.indexOf("?") + 1);
    String[] params = requestUri.split("&");
    Map<String, String> parametersMap = new HashMap<>();
    for (String param : params) {
      String[] keyValue = param.split("=");
      parametersMap.put(keyValue[0], keyValue[1]);
    }
    return parametersMap;
  }

  private HttpResponseContext dispatch(HttpRequest httpRequest)
      throws PageNotFoundException {
    String requestUri = httpRequest.uri();
    // parse the request uri to get the parameters
    String requestMapping = getRequestMapping(requestUri);
    Map<String, String> parametersMap = parseRequestParameters(requestUri);

    // parse the URI and dispatch it to different methods
    switch (requestMapping) {
      case "page":
        return doGetPage(parametersMap, httpRequest);
      case "files":
        return doListFiles(parametersMap, httpRequest);
      default:
        // TODO(JiamingMai): this should not happen, we should throw an exception here
        return null;
    }
  }

  private HttpResponseContext doGetPage(
      Map<String, String> parametersMap, HttpRequest httpRequest)
      throws PageNotFoundException {
    String fileId = parametersMap.get("fileId");
    long pageIndex = Long.parseLong(parametersMap.get("pageIndex"));

    FileRegion fileRegion = mPagedService.getPageFileRegion(fileId, pageIndex);
    HttpResponse response = new DefaultHttpResponse(httpRequest.protocolVersion(), OK);
    HttpResponseContext httpResponseContext = new HttpResponseContext(response, fileRegion);
    response.headers()
        .set(CONTENT_TYPE, TEXT_PLAIN)
        .setInt(CONTENT_LENGTH, (int) fileRegion.count());
    return httpResponseContext;
  }

  private HttpResponseContext doListFiles(Map<String, String> parametersMap,
                           HttpRequest httpRequest) {
    String path = parametersMap.get("path");
    path = handleReservedCharacters(path);
    ListStatusPOptions options = FileSystemOptionsUtils.listStatusDefaults(
        Configuration.global()).toBuilder().build();
    try {
      FileSystemContext.FileSystemContextFactory factory =
          new FileSystemContext.FileSystemContextFactory();
      FileSystemContext fileSystemContext = factory.create(Configuration.global());
      FileSystem fileSystem = FileSystem.Factory.create(fileSystemContext);

      List<URIStatus> uriStatuses = fileSystem.listStatus(new AlluxioURI(path), options);
      List<ResponseFileInfo> responseFileInfoList = new ArrayList<>();
      for (URIStatus uriStatus : uriStatuses) {
        String type = uriStatus.isFolder() ? "directory" : "file";
        ResponseFileInfo responseFileInfo = new ResponseFileInfo(type, uriStatus.getName());
        responseFileInfoList.add(responseFileInfo);
      }
      // convert to JSON string
      String responseJson = new Gson().toJson(responseFileInfoList);
      // create HTTP response
      FullHttpResponse response = new DefaultFullHttpResponse(httpRequest.protocolVersion(), OK,
          Unpooled.wrappedBuffer(responseJson.getBytes()));
      response.headers()
          .set(CONTENT_TYPE, APPLICATION_JSON)
          .setInt(CONTENT_LENGTH, response.content().readableBytes());
      return new HttpResponseContext(response, null);
    } catch (IOException | AlluxioException e) {
      LOG.error("Failed to list files of path {}", path, e);
      return null;
    }
  }

  private String handleReservedCharacters(String path) {
    path = path.replace("%2F", "/");
    return path;
  }

  private String getRequestMapping(String requestUri) {
    int endIndex = requestUri.indexOf("?");
    int startIndex = requestUri.lastIndexOf("/", endIndex);
    String requestMapping = requestUri.substring(startIndex + 1, endIndex);
    return requestMapping;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    cause.printStackTrace();
    ctx.close();
  }
}
