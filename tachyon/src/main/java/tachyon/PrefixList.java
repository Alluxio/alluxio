package tachyon;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.Validate;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

/**
 * Prefix list is used by PinList and WhiteList to do file filtering. 
 */
public class PrefixList {
  private final List<String> LIST;

  public PrefixList(List<String> prefixList) {
    if (prefixList == null) {
      LIST = new ArrayList<String>(0);
    } else {
      LIST = prefixList;
    }
  }

  public PrefixList(String prefixes, String separator) {
	  Validate.notNull(separator);
    if (prefixes == null) {
      LIST = new ArrayList<String>(0);
    } else {
      LIST = Arrays.asList(prefixes.split(separator));
    }
  }

  public boolean inList(String path) {
  	if (Strings.isNullOrEmpty(path)) {
  		return false;
  	}
  	
    for (int k = 0; k < LIST.size(); k ++) {
      if (path.startsWith(LIST.get(k))) {
        return true;
      }
    }

    return false;
  }

  public boolean outList(String path) {
    return !inList(path);
  }

  public List<String> getList() {
    return ImmutableList.copyOf(LIST);
  }
}