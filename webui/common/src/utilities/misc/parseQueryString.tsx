/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

export interface IParsedQueryString {
  [key: string]: string;
}

export const parseQueryString = (queryString: string): IParsedQueryString => {
  if (!queryString) {
    return {};
  }

  const searchArray = queryString.replace('?', '').split(/[=&]/);
  if (searchArray.length % 2 !== 0) {
    throw new Error('unable to parse querystring');
  }

  const parsedSearch: IParsedQueryString = {};
  for (let i = 0; i < searchArray.length; i += 2) {
    parsedSearch[searchArray[i]] = searchArray[i + 1];
  }
  return parsedSearch;
};
