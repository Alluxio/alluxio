package tachyon;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.Validate;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

/**
 * Prefix list is used by PinList and WhiteList to do file filtering.
 */
public class PrefixList {
  private final List<String> mInnerList;

  public PrefixList(List<String> prefixList) {
    if (prefixList == null) {
      mInnerList = new ArrayList<String>(0);
    } else {
      mInnerList = prefixList;
    }
  }

  public PrefixList(String prefixes, String separator) {
    Validate.notNull(separator);
    mInnerList = new ArrayList<String>(0);
    if (prefixes != null && !prefixes.trim().isEmpty()) {
      String[] candidates = prefixes.trim().split(separator);
      for (String prefix : candidates) {
        String trimmed = prefix.trim();
        if (!trimmed.isEmpty()) {
          mInnerList.add(trimmed);
        }
      }
    }
  }

  public List<String> getList() {
    return ImmutableList.copyOf(mInnerList);
  }

  public boolean inList(String path) {
    if (Strings.isNullOrEmpty(path)) {
      return false;
    }

    for (int k = 0; k < mInnerList.size(); k ++) {
      if (path.startsWith(mInnerList.get(k))) {
        return true;
      }
    }

    return false;
  }

  public boolean outList(String path) {
    return !inList(path);
  }

  /**
   * Print out all prefixes separated by ";".
   * 
   * @return the string representation like "a;b/c"
   */
  @Override
  public String toString() {
    StringBuilder s = new StringBuilder();
    for (String prefix : mInnerList) {
      s.append(prefix).append(";");
    }
    return s.toString();
  }
}