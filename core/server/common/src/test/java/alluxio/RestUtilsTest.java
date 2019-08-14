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

package alluxio;

import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.AlluxioStatusException;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Status;
import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.core.Response;

/**
 * Unit tests for {@link RestUtils}.
 */
public class RestUtilsTest {

  @Test
  public void voidOkResponse() {
    Response response = RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        return null;
      }
    }, ServerConfiguration.global());
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assert.assertNull(response.getEntity());
  }

  @Test
  public void stringOkResponse() throws Exception {
    final String message = "ok";
    Response response = RestUtils.call(new RestUtils.RestCallable<String>() {
      @Override
      public String call() throws Exception {
        return message;
      }
    }, ServerConfiguration.global());
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    ObjectMapper mapper = new ObjectMapper();
    String jsonMessage = mapper.writeValueAsString(message);
    Assert.assertEquals(jsonMessage, response.getEntity());
  }

  @Test
  public void objectOkResponse() throws Exception {
    class Obj {
      private final int mStatus;
      private final String mMessage;

      Obj(int status, String message) {
        mStatus = status;
        mMessage = message;
      }

      public int getStatus() {
        return mStatus;
      }

      public String getMessage() {
        return mMessage;
      }
    }

    int status = 200;
    String message = "OK";
    final Obj object = new Obj(status, message);
    Response response = RestUtils.call(new RestUtils.RestCallable<Obj>() {
      @Override
      public Obj call() throws Exception {
        return object;
      }
    }, ServerConfiguration.global());
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Obj obj = (Obj) response.getEntity();
    Assert.assertEquals(status, obj.getStatus());
    Assert.assertEquals(message, obj.getMessage());
  }

  @Test
  public void errorResponse() throws Exception {
    final Status status = Status.ALREADY_EXISTS;
    final String message = "error message";
    Response response = RestUtils.call(new RestUtils.RestCallable<Void>() {
      @Override
      public Void call() throws Exception {
        throw new AlluxioStatusException(status.withDescription(message));
      }
    }, ServerConfiguration.global());
    Assert.assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
        response.getStatus());
    RestUtils.ErrorResponse errorResponse = (RestUtils.ErrorResponse) response.getEntity();
    Assert.assertEquals(status.getCode(), errorResponse.getStatusCode());
    Assert.assertEquals(message, errorResponse.getMessage());
  }
}
