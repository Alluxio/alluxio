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

import { faAngleDoubleRight, faAngleRight } from '@fortawesome/free-solid-svg-icons';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import React from 'react';
import { Link } from 'react-router-dom';
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

export interface IPaginatorProps {
  baseUrl: string;
  path?: string;
  total: number;
  offset?: string;
  limit?: string;
}

export class Paginator extends React.PureComponent<IPaginatorProps> {
  public render(): JSX.Element | null {
    const { offset, limit, total, baseUrl, path } = this.props;

    const numOffset = this.getNumber(offset) || 0;
    const numLimit = this.getNumber(limit) || 20;

    const numPages = Math.ceil(total / numLimit);
    const firstPage = 1;
    const currentPage = numOffset ? numOffset / numLimit + 1 : 1;
    const lastPage = numPages;
    const prevPage = currentPage - 1;
    const nextPage = currentPage + 1;

    if (total <= 1) {
      return null;
    }

    return (
      <div className="paginator text-center">
        <Pagination>
          {numPages > 1 && (
            <React.Fragment>
              <PaginationItem disabled={currentPage === firstPage}>
                <PaginationLink
                  tag={Link}
                  to={`${baseUrl}?offset=0${limit ? `&limit=${limit}` : ''}${path ? `&path=${path}` : ''}`}
                >
                  <FontAwesomeIcon icon={faAngleDoubleRight} flip="horizontal" />
                </PaginationLink>
              </PaginationItem>
              <PaginationItem disabled={prevPage < 1}>
                <PaginationLink
                  tag={Link}
                  to={`${baseUrl}?offset=${(prevPage - 1) * numLimit}${limit ? `&limit=${limit}` : ''}${
                    path ? `&path=${path}` : ''
                  }`}
                >
                  <FontAwesomeIcon icon={faAngleRight} flip="horizontal" />
                </PaginationLink>
              </PaginationItem>
              <PaginationItem disabled={true}>
                <PaginationLink href="#">
                  Page {currentPage} of {numPages}
                </PaginationLink>
              </PaginationItem>
              <PaginationItem disabled={nextPage > numPages}>
                <PaginationLink
                  tag={Link}
                  to={`${baseUrl}?offset=${(nextPage - 1) * numLimit}${limit ? `&limit=${limit}` : ''}${
                    path ? `&path=${path}` : ''
                  }`}
                >
                  <FontAwesomeIcon icon={faAngleRight} />
                </PaginationLink>
              </PaginationItem>
              <PaginationItem className="mr-2" disabled={currentPage >= lastPage}>
                <PaginationLink
                  tag={Link}
                  to={`${baseUrl}?offset=${(lastPage - 1) * numLimit}${limit ? `&limit=${limit}` : ''}${
                    path ? `&path=${path}` : ''
                  }`}
                >
                  <FontAwesomeIcon icon={faAngleDoubleRight} />
                </PaginationLink>
              </PaginationItem>
            </React.Fragment>
          )}
          <PaginationItem>
            <UncontrolledButtonDropdown>
              <DropdownToggle className="bg-dark" caret={true}>
                {numLimit} Rows / Page
              </DropdownToggle>
              <DropdownMenu>
                <DropdownItem
                  tag={Link}
                  className={numLimit === 20 ? '' : 'text-success'}
                  to={`${baseUrl}?offset=${0}&limit=${20}${path ? `&path=${path}` : ''}`}
                  disabled={numLimit === 20}
                >
                  20 Rows / Page
                </DropdownItem>
                <DropdownItem
                  tag={Link}
                  className={numLimit === 100 ? '' : 'text-success'}
                  to={`${baseUrl}?offset=${0}&limit=${100}${path ? `&path=${path}` : ''}`}
                  disabled={numLimit === 100}
                >
                  100 Rows / Page
                </DropdownItem>
                <DropdownItem
                  tag={Link}
                  className={numLimit === 500 ? '' : 'text-success'}
                  to={`${baseUrl}?offset=${0}&limit=${500}${path ? `&path=${path}` : ''}`}
                  disabled={numLimit === 500}
                >
                  500 Rows / Page
                </DropdownItem>
              </DropdownMenu>
            </UncontrolledButtonDropdown>
          </PaginationItem>
        </Pagination>
      </div>
    );
  }

  private getNumber(numString?: string): number | null {
    if (numString === undefined) {
      return null;
    }

    try {
      return parseInt(numString, 10);
    } catch {
      return null;
    }
  }
}

export default Paginator;
