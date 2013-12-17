/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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