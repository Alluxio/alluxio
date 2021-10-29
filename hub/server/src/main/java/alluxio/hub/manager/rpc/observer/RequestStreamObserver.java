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

package alluxio.hub.manager.rpc.observer;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This stream observer listens for requests and returns responses to the Hosted Hub.
 *
 * @param <ReqT> request object type
 * @param <ResT> response object type
 */
public abstract class RequestStreamObserver<ReqT, ResT> implements StreamObserver<ReqT> {
  public static final Logger LOG = LoggerFactory.getLogger(RequestStreamObserver.class);

  private StreamObserver<ResT> mResponseStreamObserver;

  /**
   * Generates a response and sends it through the response stream observer.
   *
   * @param request request object to execute on
   */
  @Override
  public void onNext(ReqT request) {
    LOG.debug("Processing {} request: {}", request.getClass().toString(), request);
    ResT resp = exec(request);
    LOG.debug("Sending {} response: {}", resp.getClass().toString(), resp);
    mResponseStreamObserver.onNext(resp);
  }

  /**
   * Handles errors that occur and attempts to restart the stream observer if possible.
   *
   * @param t Throwable
   */
  @Override
  public void onError(Throwable t) {
    LOG.error("Async listener error:", t);
    LOG.info("Async listener connection canceled. Attempting to reconnect...");
    restart();
  }

  /**
   * Properly shutdown the stream observer.
   */
  @Override
  public void onCompleted() {
    LOG.error("Async listener closed");
  }

  /**
   * Initialize the response stream observer.
   *
   * @param responseStreamObserver response stream observer
   * @param resp response object
   */
  public void start(StreamObserver<ResT> responseStreamObserver, ResT resp) {
    mResponseStreamObserver = responseStreamObserver;
    mResponseStreamObserver.onNext(resp);
  }

  /**
   * Executes on a request to return a response.
   *
   * @param req request object to execute on
   * @return response object
   */
  public abstract ResT exec(ReqT req);

  /**
   * Handles restarting the request stream observer.
   */
  public abstract void restart();
}
