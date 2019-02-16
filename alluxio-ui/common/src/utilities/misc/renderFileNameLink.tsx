import {Link} from 'react-router-dom';
import React from 'react';

export const renderFileNameLink = function (this: any, path: string, url: string) {
  const {lastFetched} = this.state;
  if (path === lastFetched.path) {
    return (
      path
    );
  }

  return (
    <pre className="mb-0">
      <code>
      <Link to={url}>
        {path}
      </Link>
    </code>
    </pre>
  );
};
