package alluxio.master.metastore.rocks;

import java.util.List;
import java.util.concurrent.Future;

import static org.junit.Assert.fail;

public class RocksStoreTestUtils {
  public static void waitForReaders(List<Future<Void>> futures) {
    futures.stream().forEach(f -> {
      try {
        f.get();
      } catch (Exception e) {
        fail("Met uncaught exception from iteration");
      }
    });
  }
}
