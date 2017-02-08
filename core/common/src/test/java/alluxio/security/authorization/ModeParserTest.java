package alluxio.security.authorization;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for the {@code ModeParser}.
 * @author rvesse
 *
 */
public final class ModeParserTest {

  /**
   * The exception expected to be thrown.
   */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();
  
  private final ModeParser parser = new ModeParser();
  
  @Test
  public void numerics() {
    Mode parsed = parser.parse("777");
    Assert.assertEquals(Mode.Bits.ALL, parsed.getOwnerBits());
    Assert.assertEquals(Mode.Bits.ALL, parsed.getGroupBits());
    Assert.assertEquals(Mode.Bits.ALL, parsed.getOtherBits());

    parsed = parser.parse("755");
    Assert.assertEquals(Mode.Bits.ALL, parsed.getOwnerBits());
    Assert.assertEquals(Mode.Bits.READ_EXECUTE, parsed.getGroupBits());
    Assert.assertEquals(Mode.Bits.READ_EXECUTE, parsed.getOtherBits());
    
    parsed = parser.parse("644");
    Assert.assertEquals(Mode.Bits.READ_WRITE, parsed.getOwnerBits());
    Assert.assertEquals(Mode.Bits.READ, parsed.getGroupBits());
    Assert.assertEquals(Mode.Bits.READ, parsed.getOtherBits());
  }
  
  @Test
  public void symbolics_separated() {
    Mode parsed = parser.parse("u=rwx,g=rwx,o=rwx");
    Assert.assertEquals(Mode.Bits.ALL, parsed.getOwnerBits());
    Assert.assertEquals(Mode.Bits.ALL, parsed.getGroupBits());
    Assert.assertEquals(Mode.Bits.ALL, parsed.getOtherBits());

    parsed = parser.parse("u=rwx,g=rx,o=rx");
    Assert.assertEquals(Mode.Bits.ALL, parsed.getOwnerBits());
    Assert.assertEquals(Mode.Bits.READ_EXECUTE, parsed.getGroupBits());
    Assert.assertEquals(Mode.Bits.READ_EXECUTE, parsed.getOtherBits());
    
    parsed = parser.parse("u=rw,g=r,o=r");
    Assert.assertEquals(Mode.Bits.READ_WRITE, parsed.getOwnerBits());
    Assert.assertEquals(Mode.Bits.READ, parsed.getGroupBits());
    Assert.assertEquals(Mode.Bits.READ, parsed.getOtherBits());
  }
  
  @Test
  public void symbolics_combined() {
    Mode parsed = parser.parse("ugo=rwx");
    Assert.assertEquals(Mode.Bits.ALL, parsed.getOwnerBits());
    Assert.assertEquals(Mode.Bits.ALL, parsed.getGroupBits());
    Assert.assertEquals(Mode.Bits.ALL, parsed.getOtherBits());

    parsed = parser.parse("u=rwx,go=rx");
    Assert.assertEquals(Mode.Bits.ALL, parsed.getOwnerBits());
    Assert.assertEquals(Mode.Bits.READ_EXECUTE, parsed.getGroupBits());
    Assert.assertEquals(Mode.Bits.READ_EXECUTE, parsed.getOtherBits());
    
    parsed = parser.parse("u=rw,go=r");
    Assert.assertEquals(Mode.Bits.READ_WRITE, parsed.getOwnerBits());
    Assert.assertEquals(Mode.Bits.READ, parsed.getGroupBits());
    Assert.assertEquals(Mode.Bits.READ, parsed.getOtherBits());
  }
}
