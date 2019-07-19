import {Link} from 'react-router-dom';
import React from 'react';

export const renderFileNameLink = function (path: string, urlPrefix: string): JSX.Element {
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
