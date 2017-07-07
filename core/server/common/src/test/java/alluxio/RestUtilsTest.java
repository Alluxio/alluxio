package alluxio;

import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.Status;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Objects;

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

      public Obj(int status, String message) {
        mStatus = status;
        mMessage = message;
      }

      public int getStatus() {
        return mStatus;
      }

      public String getMessage() {
        return mMessage;
      }

      @Override
      public boolean equals(Object obj) {
        if (obj == null) {
          return false;
        }
        if (!(obj instanceof Obj)) {
          return false;
        }
        Obj that = (Obj) obj;
        return Objects.equals(mStatus, that.getStatus()) &&
            Objects.equals(mMessage, that.getMessage());
      }
    }

    final Obj object = new Obj(200, "OK");
    Response response = RestUtils.call(new RestUtils.RestCallable<Obj>() {
      @Override
      public Obj call() throws Exception {
        return object;
      }
    });
    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assert.assertEquals(object, response.getEntity());
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
