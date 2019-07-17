import {Link} from 'react-router-dom';
import React from 'react';

export const renderFileNameLink = function (this: any, path: string, urlPrefix: string) {
  const {lastFetched} = this.state;
  if (path === lastFetched.path) {
    return (
      path
    );
  }
  const encodedPath = encodeURIComponent(path) || "";
  const encodedUrl = urlPrefix + encodedPath;

  return (
    <pre className="mb-0">
      <code>
      <Link to={encodedUrl}>
        {path}
      </Link>
    </code>
    </pre>
  );
};
