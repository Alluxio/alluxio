package tachyon.hadoop;

import java.util.regex.PatternSyntaxException;
import java.io.IOException;

import org.apache.hadoop.fs.GlobPattern;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * A class that could decide if a string matches the glob or not.
 * 
 * This was copied from Hadoop 0.20.205
 * @author haoyuan
 */
class GlobFilter implements PathFilter {
  private static final PathFilter DEFAULT_FILTER = new PathFilter() {
    public boolean accept(Path file) {
      return true;
    }
  };

  private PathFilter userFilter = DEFAULT_FILTER;
  private GlobPattern pattern;

  GlobFilter(String filePattern) throws IOException {
    init(filePattern, DEFAULT_FILTER);
  }

  GlobFilter(String filePattern, PathFilter filter) throws IOException {
    init(filePattern, filter);
  }

  void init(String filePattern, PathFilter filter) throws IOException {
    try {
      userFilter = filter;
      pattern = new GlobPattern(filePattern);
    }
    catch (PatternSyntaxException e) {
      // Existing code expects IOException startWith("Illegal file pattern")
      throw new IOException("Illegal file pattern: "+ e.getMessage(), e);
    }
  }

  boolean hasPattern() {
    return pattern.hasWildcard();
  }

  public boolean accept(Path path) {
    return pattern.matches(path.getName()) && userFilter.accept(path);
  }
}