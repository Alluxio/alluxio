import {faFastForward, faForward} from '@fortawesome/free-solid-svg-icons';
import {FontAwesomeIcon} from '@fortawesome/react-fontawesome'
import React from 'react';
import {Pagination, PaginationItem, PaginationLink} from 'reactstrap';

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
            <PaginationLink href={`${baseUrl}?offset=0${limit ? `&limit=${limit}` : ''}${path ? `&path=${path}` : ''}`}>
              <FontAwesomeIcon icon={faFastForward} flip="horizontal"/>
            </PaginationLink>
          </PaginationItem>
          <PaginationItem disabled={prevPage < 1}>
            <PaginationLink
              href={`${baseUrl}?offset=${prevPage * numLimit}${limit ? `&limit=${limit}` : ''}${path ? `&path=${path}` : ''}`}>
              <FontAwesomeIcon icon={faForward} flip="horizontal"/>
            </PaginationLink>
          </PaginationItem>
          <PaginationItem disabled={true}>
            <PaginationLink href="#">
              Page {currentPage} of {numPages}
            </PaginationLink>
          </PaginationItem>
          <PaginationItem disabled={nextPage > numPages}>
            <PaginationLink
              href={`${baseUrl}?offset=${(nextPage - 1) * numLimit}${limit ? `&limit=${limit}` : ''}${path ? `&path=${path}` : ''}`}>
              <FontAwesomeIcon icon={faForward}/>
            </PaginationLink>
          </PaginationItem>
          <PaginationItem disabled={currentPage === lastPage}>
            <PaginationLink
              href={`${baseUrl}?offset=${(lastPage - 1) * numLimit}${limit ? `&limit=${limit}` : ''}${path ? `&path=${path}` : ''}`}>
              <FontAwesomeIcon icon={faFastForward}/>
            </PaginationLink>
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
