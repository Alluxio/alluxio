package tachyon.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import tachyon.Constants;

public class CommonUtilsTest {
  @Test
  public void getPathWithoutSchemaTest() {
    List<String> schemas = new ArrayList<String>();
    schemas.add("");
    schemas.add("tachyon://abc:19998");
    schemas.add("tachyon-ft://abc:19998");
    schemas.add("tachyon://localhost:19998");
    schemas.add("tachyon-ft://localhost:19998");
    schemas.add("tachyon://127.0.0.1:19998");
    schemas.add("tachyon-ft://127.0.0.1:19998");
    for (int k = 0; k < schemas.size(); k ++) {
      String schema = schemas.get(k);
      if (!schema.equals("")) {
        Assert.assertEquals("/", CommonUtils.getPathWithoutSchema(schema));
      }
      Assert.assertEquals("/", CommonUtils.getPathWithoutSchema(schema + "/"));
      Assert.assertEquals("/123", CommonUtils.getPathWithoutSchema(schema + "/123"));
      Assert.assertEquals("/ab/de.txt", CommonUtils.getPathWithoutSchema(schema + "/ab/de.txt"));
    }
  }

  @Test
  public void parseSpaceSizeTest() {
    long max = 10240;
    for (long k = 0; k < max; k ++) {
      Assert.assertEquals(k / 10, CommonUtils.parseSpaceSize(k / 10.0 + "b"));
      Assert.assertEquals(k / 10, CommonUtils.parseSpaceSize(k / 10.0 + "B"));
      Assert.assertEquals(k / 10, CommonUtils.parseSpaceSize(k / 10.0 + ""));
    }
    for (long k = 0; k < max; k ++) {
      Assert.assertEquals(k * Constants.KB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "kb"));
      Assert.assertEquals(k * Constants.KB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "Kb"));
      Assert.assertEquals(k * Constants.KB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "KB"));
      Assert.assertEquals(k * Constants.KB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "kB"));
    }
    for (long k = 0; k < max; k ++) {
      Assert.assertEquals(k * Constants.MB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "mb"));
      Assert.assertEquals(k * Constants.MB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "Mb"));
      Assert.assertEquals(k * Constants.MB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "MB"));
      Assert.assertEquals(k * Constants.MB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "mB"));
    }
    for (long k = 0; k < max; k ++) {
      Assert.assertEquals(k * Constants.GB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "gb"));
      Assert.assertEquals(k * Constants.GB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "Gb"));
      Assert.assertEquals(k * Constants.GB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "GB"));
      Assert.assertEquals(k * Constants.GB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "gB"));
    }
    for (long k = 0; k < max; k ++) {
      Assert.assertEquals(k * Constants.TB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "tb"));
      Assert.assertEquals(k * Constants.TB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "Tb"));
      Assert.assertEquals(k * Constants.TB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "TB"));
      Assert.assertEquals(k * Constants.TB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "tB"));
    }
    // We stop the pb test before 8192, since 8192 petabytes is beyond the scope of a java long.
    for (long k = 0; k < 8192; k ++) {
      Assert.assertEquals(k * Constants.PB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "pb"));
      Assert.assertEquals(k * Constants.PB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "Pb"));
      Assert.assertEquals(k * Constants.PB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "PB"));
      Assert.assertEquals(k * Constants.PB / 10, CommonUtils.parseSpaceSize(k / 10.0 + "pB"));
    }
  }

  @Test
  public void convertToClockTimeWithShortValue() {
    String out = CommonUtils.convertMsToClockTime(10);
    Assert.assertEquals("0 day(s), 0 hour(s), 0 minute(s), and 0 second(s)", out);
  }

  @Test
  public void convertToClockTimeWithOneSecond() {
    String out = CommonUtils.convertMsToClockTime(TimeUnit.SECONDS.toMillis(1));
    Assert.assertEquals("0 day(s), 0 hour(s), 0 minute(s), and 1 second(s)", out);
  }

  @Test
  public void convertToClockTimeWithOneMinute() {
    String out = CommonUtils.convertMsToClockTime(TimeUnit.MINUTES.toMillis(1));
    Assert.assertEquals("0 day(s), 0 hour(s), 1 minute(s), and 0 second(s)", out);
  }

  @Test
  public void convertToClockTimeWithOneMinute30Seconds() {
    String out =
        CommonUtils.convertMsToClockTime(TimeUnit.MINUTES.toMillis(1)
            + TimeUnit.SECONDS.toMillis(30));
    Assert.assertEquals("0 day(s), 0 hour(s), 1 minute(s), and 30 second(s)", out);
  }

  @Test
  public void convertToClockTimeWithOneHour() {
    String out = CommonUtils.convertMsToClockTime(TimeUnit.HOURS.toMillis(1));
    Assert.assertEquals("0 day(s), 1 hour(s), 0 minute(s), and 0 second(s)", out);
  }

  @Test
  public void convertToClockTimeWithOneHour10Minutes45Seconds() {
    String out =
        CommonUtils.convertMsToClockTime(TimeUnit.HOURS.toMillis(1) + TimeUnit.MINUTES.toMillis(10)
            + TimeUnit.SECONDS.toMillis(45));
    Assert.assertEquals("0 day(s), 1 hour(s), 10 minute(s), and 45 second(s)", out);
  }

  @Test
  public void convertToClockTimeWithOneDay() {
    String out = CommonUtils.convertMsToClockTime(TimeUnit.DAYS.toMillis(1));
    Assert.assertEquals("1 day(s), 0 hour(s), 0 minute(s), and 0 second(s)", out);
  }

  @Test
  public void convertToClockTimeWithOneDay4Hours10Minutes45Seconds() {
    long time =
        TimeUnit.DAYS.toMillis(1) + TimeUnit.HOURS.toMillis(4) + TimeUnit.MINUTES.toMillis(10)
            + TimeUnit.SECONDS.toMillis(45);
    String out = CommonUtils.convertMsToClockTime(time);
    Assert.assertEquals("1 day(s), 4 hour(s), 10 minute(s), and 45 second(s)", out);
  }

  @Test
  public void convertToClockTimeWithOneDay4Hours10Minutes45SecondsWithStopwatch() {
    long time =
        TimeUnit.DAYS.toMillis(1) + TimeUnit.HOURS.toMillis(4) + TimeUnit.MINUTES.toMillis(10)
            + TimeUnit.SECONDS.toMillis(45);
    String out = CommonUtils.convertMsToClockTime(time);
    Assert.assertEquals("1 day(s), 4 hour(s), 10 minute(s), and 45 second(s)", out);
  }

  @Test(expected = IllegalArgumentException.class)
  public void convertToClockTimeWithNegativeValue() {
    CommonUtils.convertMsToClockTime(1 - TimeUnit.DAYS.toMillis(1) + TimeUnit.HOURS.toMillis(4)
        + TimeUnit.MINUTES.toMillis(10) + TimeUnit.SECONDS.toMillis(45));
  }
}
