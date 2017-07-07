package alluxio;

import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.Status;

import com.fasterxml.jackson.databind.ObjectMapper;
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
    });
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
    });
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
    });
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
        throw new AlluxioStatusException(status, message);
      }
    });
    Assert.assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
        response.getStatus());
    RestUtils.ErrorResponse errorResponse = (RestUtils.ErrorResponse) response.getEntity();
    Assert.assertEquals(status, errorResponse.getStatus());
    Assert.assertEquals(message, errorResponse.getMessage());
  }
}
