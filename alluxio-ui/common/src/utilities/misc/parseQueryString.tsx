export interface IParsedQueryString {
  [key: string]: string;
}

export const parseQuerystring = (searchString: string): IParsedQueryString => {
  if (!searchString) { return {}; }

  const searchArray = searchString.replace('?', '').split(/[=&]/);
  if (searchArray.length % 2 !== 0) {
    throw new Error('unable to parse querystring');
  }

  const parsedSearch = {};
  for (let i = 0; i < searchArray.length; i += 2) {
    parsedSearch[searchArray[i]] = searchArray[i + 1];
  }
  return parsedSearch;
};
