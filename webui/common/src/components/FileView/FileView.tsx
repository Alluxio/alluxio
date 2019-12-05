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

import { faCheckSquare, faSquare } from '@fortawesome/free-regular-svg-icons';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { History, LocationState } from 'history';
import React from 'react';
import { Link } from 'react-router-dom';
import { Button, ButtonGroup, Form, FormGroup, Input, Label } from 'reactstrap';

import { IFileViewData } from '../../constants';
import { disableFormSubmit } from '../../utilities';

export interface IFileViewProps {
  allowDownload?: boolean;
  beginInputHandler: (event: React.MouseEvent<HTMLButtonElement>) => void;
  end?: string;
  endInputHandler: (event: React.MouseEvent<HTMLButtonElement>) => void;
  history: History<LocationState>;
  limit?: string;
  offset?: string;
  offsetInputHandler: (event: React.ChangeEvent<HTMLInputElement>) => void;
  path?: string;
  queryStringPrefix: string;
  queryStringSuffix: string;
  textAreaHeight?: number;
  viewData: IFileViewData;
  proxyDownloadApiUrl?: {
    prefix: string;
    suffix: string;
  };
}

export class FileView extends React.PureComponent<IFileViewProps> {
  public render(): JSX.Element {
    const {
      beginInputHandler,
      end,
      endInputHandler,
      offset,
      offsetInputHandler,
      path,
      queryStringPrefix,
      queryStringSuffix,
      textAreaHeight,
      viewData,
      history,
    } = this.props;

    return (
      <React.Fragment>
        <h5>
          {viewData.currentDirectory ? viewData.currentDirectory.absolutePath : viewData.currentPath}
          {': '}
          <small>First 5KB from {viewData.viewingOffset} in ASCII</small>
        </h5>
        <Form className="mb-3 viewData-file-form" id="viewDataFileForm" inline={true} onSubmit={disableFormSubmit}>
          <FormGroup className="mb-2 mr-sm-2 w-100">
            <Input
              className="w-100"
              type="textarea"
              value={viewData.fileData || ''}
              style={{ height: textAreaHeight }}
              readOnly={true}
            />
          </FormGroup>
        </Form>
        <hr />
        <Form
          className="mb-3 viewData-file-settings-form"
          id="viewDataFileSettingsForm"
          inline={true}
          onSubmit={disableFormSubmit}
        >
          <FormGroup className="col-5">
            <Label for="viewDataFileOffset" className="mr-sm-2">
              Display from byte offset
            </Label>
            <Input
              className="col-3"
              type="number"
              id="viewDataFileOffset"
              placeholder="Enter an offset"
              value={offset}
              onChange={offsetInputHandler}
              onKeyUp={this.createInputEnterHandler(
                history,
                () => `${queryStringPrefix}?path=${path}${queryStringSuffix}`,
              )}
              onFocus={(evt: React.FocusEvent<HTMLInputElement>): void => evt.currentTarget.select()}
            />
          </FormGroup>
          <FormGroup className="col-5">
            <Label for="viewDataFileEnd" className="mr-sm-2">
              Relative to
            </Label>
            <ButtonGroup id="viewDataFileEnd" className="auto-refresh-button">
              <Button size="sm" outline={!!end} color="secondary" onClick={beginInputHandler}>
                <FontAwesomeIcon icon={!end ? faCheckSquare : faSquare} />
                &nbsp;begin
              </Button>
              <Button size="sm" outline={!end} color="secondary" onClick={endInputHandler}>
                <FontAwesomeIcon icon={end ? faCheckSquare : faSquare} />
                &nbsp;end
              </Button>
            </ButtonGroup>
          </FormGroup>
          <FormGroup className="col-2">
            <Button
              tag={Link}
              to={`${queryStringPrefix}?path=${encodeURIComponent(path || '')}${queryStringSuffix}`}
              color="secondary"
            >
              Go
            </Button>
          </FormGroup>
          {this.renderDownloadLink()}
        </Form>
      </React.Fragment>
    );
  }

  private createInputEnterHandler(
    history: History<LocationState>,
    stateValueCallback: (value: string) => string | undefined,
  ) {
    return (event: React.KeyboardEvent<HTMLInputElement>): void => {
      const value = event.key;
      if (event.key === 'Enter') {
        const newPath = stateValueCallback(value);
        if (newPath) {
          if (history) {
            history.push(newPath);
          }
        }
      }
    };
  }

  private renderDownloadLink(): JSX.Element | null {
    const { allowDownload, path, proxyDownloadApiUrl } = this.props;

    if (allowDownload && proxyDownloadApiUrl && path) {
      return (
        <FormGroup className="col-4 mt-2">
          <a href={`${proxyDownloadApiUrl.prefix}${path}${proxyDownloadApiUrl.suffix}`}>Download via Alluxio Proxy</a>
        </FormGroup>
      );
    }

    return null;
  }
}
