package tachyon;

import java.util.ArrayList;
import java.util.List;

/**
 * Prefix list is used by PINList and WhiteList to do file filter. 
 */
public class PrefixList {
  private final List<String> LIST;

  public PrefixList(ArrayList<String> prefixList) {
    LIST = prefixList;
  }

  public boolean inList(String path) {
    for (int k = 0; k < LIST.size(); k ++) {
      if (path.startsWith(LIST.get(k))) {
        return true;
      }
    }

    return false;
  }
  
  public List<String> getList() {
    return new ArrayList<String>(LIST);
  }
}