package tachyon.perf.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * File list generator for read and write test.
 */
public class ListGenerator {
  private static Random sRand = new Random(System.currentTimeMillis());

  /**
   * Randomly select from the candidates. This method is designed to generate a list of read file
   * names but the logic can be used in other situations.
   * 
   * @param filesNum the target number of files
   * @param candidates
   * @return the generated list of read file names
   */
  public static List<String> generateRandomReadFiles(int filesNum, List<String> candidates) {
    List<String> ret = new ArrayList<String>(filesNum);
    int range = candidates.size();
    for (int i = 0; i < filesNum; i ++) {
      ret.add(candidates.get(sRand.nextInt(range)));
    }
    return ret;
  }

  /**
   * Sequentially select from the candidates. This method is designed to generate a list of read
   * file names but the logic can be used in other situations.
   * 
   * @param id the id of the thread
   * @param threadsNum the total threads number
   * @param filesNum the target number of files
   * @param candidates
   * @return the generated list of read file names
   */
  public static List<String> generateSequenceReadFiles(int id, int threadsNum, int filesNum,
      List<String> candidates) {
    List<String> ret = new ArrayList<String>(filesNum);
    int range = candidates.size();
    int index = range / threadsNum * id;
    for (int i = 0; i < filesNum; i ++) {
      ret.add(candidates.get(index));
      index = (index + 1) % range;
    }
    return ret;
  }

  /**
   * generate a list of write file names
   * 
   * @param id the id of the thread
   * @param filesNum the target number of files
   * @param dirPrefix all the files are under this directory
   * @return the generated list of write file names
   */
  public static List<String> generateWriteFiles(int id, int filesNum, String dirPrefix) {
    List<String> ret = new ArrayList<String>(filesNum);
    for (int i = 0; i < filesNum; i ++) {
      ret.add(dirPrefix + "/" + id + "-" + i);
    }
    return ret;
  }
}
