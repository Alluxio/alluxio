package tachyon;

import java.util.ArrayList;
import java.util.List;

/**
 * A file can only be cached if it passes the white list.
 * @author Haoyuan
 */
public class PrefixList {
  private final List<String> LIST;

  public PrefixList(ArrayList<String> prefixList) {
    LIST = prefixList;
  }

  public boolean inList(String datasetPath) {
    for (int k = 0; k < LIST.size(); k ++) {
      if (datasetPath.startsWith(LIST.get(k))) {
        return true;
      }
    }

    return false;
  }
  
  public List<String> getList() {
    return new ArrayList<String>(LIST);
  }

  public String toHtml(String listName) {
    StringBuilder sb = new StringBuilder("<h2> " + listName + " contains " + LIST.size() + 
        " item(s) </h2>");

    for (int k = 0; k < LIST.size(); k ++) {
      String item = LIST.get(k);
      sb.append("Prefix " + (k + 1) + " : " + item + " <br \\>");
    }
    sb.append(" <br \\>");

    return sb.toString();
  }
}