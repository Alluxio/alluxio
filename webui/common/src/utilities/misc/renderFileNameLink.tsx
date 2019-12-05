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

import { Link } from 'react-router-dom';
import React from 'react';

export const renderFileNameLink = function(path: string, urlPrefix: string): JSX.Element {
  const encodedPath = encodeURIComponent(path) || '';
  const encodedUrl = urlPrefix + encodedPath;

  return (
    <pre className="mb-0">
      <code>
        <Link to={encodedUrl}>{path}</Link>
      </code>
    </pre>
  );
};
