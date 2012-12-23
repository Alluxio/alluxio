package tachyon;

import java.util.ArrayList;

/**
 * A file can only be cached if it passes the white list.
 * @author Haoyuan
 */
public class PrefixList {
  private ArrayList<String> mPrefixList = new ArrayList<String>();

  public PrefixList(ArrayList<String> prefixList) {
    mPrefixList = prefixList;
  }

  public boolean inList(String datasetPath) {
    for (int k = 0; k < mPrefixList.size(); k ++) {
      if (datasetPath.startsWith(mPrefixList.get(k))) {
        return true;
      }
    }

    return false;
  }

  public String toHtml(String listName) {
    StringBuilder sb = new StringBuilder("<h2> " + listName + " contains " + mPrefixList.size() + 
        " item(s) </h2>");

    for (int k = 0; k < mPrefixList.size(); k ++) {
      String item = mPrefixList.get(k);
      sb.append("Prefix " + (k + 1) + " : " + item + " <br \\>");
    }
    sb.append(" <br \\>");

    return sb.toString();
  }
}