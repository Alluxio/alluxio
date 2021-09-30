package alluxio.client.file.cache.filter;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BuiltinBitSetTest {
    @Test
    public void basicTest() {
        AbstractBitSet bitSet = new BuiltinBitSet(100);
        bitSet.set(9);
        assertTrue(bitSet.get(9));
        assertFalse(bitSet.get(8));
    }
}
