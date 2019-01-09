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

import {faFastForward, faForward} from '@fortawesome/free-solid-svg-icons';
import {FontAwesomeIcon} from '@fortawesome/react-fontawesome'
import React from 'react';
import {Link} from 'react-router-dom';
import {
  DropdownItem,
  DropdownMenu,
  DropdownToggle,
  Pagination,
  PaginationItem,
  PaginationLink,
  UncontrolledButtonDropdown,
} from 'reactstrap';

import './Paginator.css';

interface IPaginagorProps {
  baseUrl: string;
  path?: string;
  total: number;
  offset?: string;
  limit?: string;
}

export class Paginator extends React.PureComponent<IPaginagorProps> {
  public render() {
    const {offset, limit, total, baseUrl, path} = this.props;

    const numOffset = this.getNumber(offset);
    const numLimit = this.getNumber(limit) || 20;

    if (numOffset === null || numLimit === null) {
      return null;
    }

    const numPages = Math.ceil(total / numLimit);
    const firstPage = 1;
    const currentPage = numOffset ? Math.ceil(numPages / numOffset) : firstPage;
    const lastPage = numPages;
    const prevPage = currentPage - 1;
    const nextPage = currentPage + 1;

    if (numPages === 1) {
      return null;
    }

    return (
      <div className="paginator text-center">
        <Pagination>
          <PaginationItem disabled={currentPage === firstPage}>
            <PaginationLink tag={Link}
                            to={`${baseUrl}?offset=0${limit ? `&limit=${limit}` : ''}${path ? `&path=${path}` : ''}`}>
              <FontAwesomeIcon icon={faFastForward} flip="horizontal"/>
            </PaginationLink>
          </PaginationItem>
          <PaginationItem disabled={prevPage < 1}>
            <PaginationLink tag={Link}
                            to={`${baseUrl}?offset=${prevPage * numLimit}${limit ? `&limit=${limit}` : ''}${path ? `&path=${path}` : ''}`}>
              <FontAwesomeIcon icon={faForward} flip="horizontal"/>
            </PaginationLink>
          </PaginationItem>
          <PaginationItem disabled={true}>
            <PaginationLink href="#">
              Page {currentPage} of {numPages}
            </PaginationLink>
          </PaginationItem>
          <PaginationItem disabled={nextPage > numPages}>
            <PaginationLink tag={Link}
                            to={`${baseUrl}?offset=${(nextPage - 1) * numLimit}${limit ? `&limit=${limit}` : ''}${path ? `&path=${path}` : ''}`}>
              <FontAwesomeIcon icon={faForward}/>
            </PaginationLink>
          </PaginationItem>
          <PaginationItem disabled={currentPage === lastPage}>
            <PaginationLink tag={Link}
                            to={`${baseUrl}?offset=${(lastPage - 1) * numLimit}${limit ? `&limit=${limit}` : ''}${path ? `&path=${path}` : ''}`}>
              <FontAwesomeIcon icon={faFastForward}/>
            </PaginationLink>
          </PaginationItem>
          <PaginationItem className="ml-2">
            <UncontrolledButtonDropdown>
              <DropdownToggle caret={true}>
                View {numLimit} rows
              </DropdownToggle>
              <DropdownMenu>
                <DropdownItem tag={Link}
                              to={`${baseUrl}?offset=${0}&limit=${20}${path ? `&path=${path}` : ''}`}
                              disabled={numLimit === 20}>View 20 rows</DropdownItem>
                <DropdownItem tag={Link}
                              to={`${baseUrl}?offset=${0}&limit=${100}${path ? `&path=${path}` : ''}`}
                              disabled={numLimit === 100}>View 100 rows</DropdownItem>
                <DropdownItem tag={Link}
                              to={`${baseUrl}?offset=${0}&limit=${500}${path ? `&path=${path}` : ''}`}
                              disabled={numLimit === 500}>View 500 rows</DropdownItem>
              </DropdownMenu>
            </UncontrolledButtonDropdown>
          </PaginationItem>
        </Pagination>
      </div>
    );
  }

  private getNumber(numString?: string) {
    if (!numString) {
      return 0;
    }

    try {
      return parseInt(numString, 10);
    } catch {
      return null;
    }
  }
}

export default Paginator;
