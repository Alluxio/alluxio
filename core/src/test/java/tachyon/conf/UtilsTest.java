package tachyon.conf;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public final class UtilsTest {
  private static final ImmutableList<String> EMPTY = ImmutableList.of();

  @Test
  public void listPropertyTest() {
    ImmutableList<String> expected = ImmutableList.of("foo", "bar", "baz");
    String key = "listPropertyTest.1";
    System.setProperty(key, "foo bar baz");
    ImmutableList<String> out = Utils.getListProperty(key, EMPTY);

    Assert.assertEquals(expected, out);
  }

  @Test
  public void listPropertyCommaTest() {
    ImmutableList<String> expected = ImmutableList.of("foo", "bar", "baz");
    String key = "listPropertyTest.2";
    System.setProperty(key, "foo,bar,baz");
    ImmutableList<String> out = Utils.getListProperty(key, EMPTY);

    Assert.assertEquals(expected, out);
  }

  @Test
  public void listPropertyMixModeTest() {
    ImmutableList<String> expected = ImmutableList.of("foo", "bar", "baz", "biz");
    String key = "listPropertyTest.2";
    System.setProperty(key, "foo,bar baz\tbiz");
    ImmutableList<String> out = Utils.getListProperty(key, EMPTY);

    Assert.assertEquals(expected, out);
  }

  @Test
  public void listPropertyHeavyFormatTest() {
    ImmutableList<String> expected = ImmutableList.of("foo", "bar", "baz", "biz");
    String key = "listPropertyTest.2";
    System.setProperty(key, "foo\n\n\n\t\r\tbar\n\n\n\t\n\r\nbaz\tbiz");
    ImmutableList<String> out = Utils.getListProperty(key, EMPTY);

    Assert.assertEquals(expected, out);
  }

  @Test
  public void listPropertiesMissingTest() {
    ImmutableList<String> expected = ImmutableList.of("foo", "bar", "baz");
    ImmutableList<String> out = Utils.getListProperty("this should so be missing!", expected);

    Assert.assertEquals(expected, out);
  }

}
