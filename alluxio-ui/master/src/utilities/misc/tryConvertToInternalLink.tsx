export const tryConvertToInternalLink = (url: string) => url.replace(/^http(s)?\:\/\/(www\.)?alluxio.org\//i, '/');
