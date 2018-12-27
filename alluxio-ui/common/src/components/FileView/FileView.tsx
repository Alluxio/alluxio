import {faCheckSquare, faSquare} from '@fortawesome/free-regular-svg-icons'
import {FontAwesomeIcon} from '@fortawesome/react-fontawesome'
import React from 'react';
import {Link} from 'react-router-dom';
import {Button, ButtonGroup, Form, FormGroup, Input, Label} from 'reactstrap';

import {IFileInfo} from '../../constants';

interface IFileViewProps {
  allowDownload?: boolean;
  beginInputHandler: (event: React.MouseEvent<HTMLButtonElement>) => void;
  end?: string
  endInputHandler: (event: React.MouseEvent<HTMLButtonElement>) => void;
  lastFetched: {
    end?: string;
    limit?: string;
    offset?: string;
    path?: string;
  };
  limit?: string;
  offset?: string;
  offsetInputHandler: (event: React.ChangeEvent<HTMLInputElement>) => void;
  path?: string;
  queryStringPrefix: string;
  queryStringSuffix: string;
  textAreaHeight?: number;
  viewData: {
    currentDirectory?: IFileInfo;
    'currentPath': string;
    'debug': boolean;
    'fatalError': string;
    'fileData': string;
    'fileInfos': IFileInfo[];
    'invalidPathError': string;
    'ntotalFile': number;
    'viewingOffset': number;
  }
}

export class FileView extends React.PureComponent<IFileViewProps> {
  constructor(props: IFileViewProps) {
    super(props);
  }

public render(): JSX.Element {
    const {
      allowDownload,
      beginInputHandler,
      end,
      endInputHandler,
      lastFetched,
      offset,
      offsetInputHandler,
      path,
      queryStringPrefix,
      queryStringSuffix,
      textAreaHeight,
      viewData
    } = this.props;

    return (
      <React.Fragment>
        <h5>
          {viewData.currentDirectory ? viewData.currentDirectory.absolutePath : viewData.currentPath}: <small>First 5KB from {viewData.viewingOffset} in ASCII</small>
        </h5>
        <Form className="mb-3 viewData-file-form" id="viewDataFileForm" inline={true}>
          <FormGroup className="mb-2 mr-sm-2 w-100">
            <Input className="w-100" type="textarea" value={viewData.fileData} style={{height: textAreaHeight}}
                   readOnly={true}/>
          </FormGroup>
        </Form>
        <hr/>
        <Form className="mb-3 viewData-file-settings-form" id="viewDataFileSettingsForm" inline={true}>
          <FormGroup className="col-5">
            <Label for="viewDataFileOffset" className="mr-sm-2">Display from byte offset</Label>
            <Input className="col-3" type="text" id="viewDataFileOffset" placeholder="Enter an offset"
                   value={offset || '0'}
                   onChange={offsetInputHandler}/>
          </FormGroup>
          <FormGroup className="col-5">
            <Label for="viewDataFileEnd" className="mr-sm-2">Relative to</Label>
            <ButtonGroup id="viewDataFileEnd" className="auto-refresh-button">
              <Button size="sm" outline={!!end} color="secondary" onClick={beginInputHandler}>
                <FontAwesomeIcon icon={!end ? faCheckSquare : faSquare}/>&nbsp;begin
              </Button>
              <Button size="sm" outline={!end} color="secondary" onClick={endInputHandler}>
                <FontAwesomeIcon icon={!!end ? faCheckSquare : faSquare}/>&nbsp;end
              </Button>
            </ButtonGroup>
          </FormGroup>
          <FormGroup className="col-2">
            <Button tag={Link} to={`${queryStringPrefix}?path=${path}${queryStringSuffix}`} color="primary"
                    disabled={offset === lastFetched.offset && end === lastFetched.end}>Go</Button>
          </FormGroup>
          {allowDownload && <FormGroup className="col-4">
            <a href={`${process.env.REACT_APP_API_DOWNLOAD}?path=${encodeURIComponent(path || '')}`}>
              Download
            </a>
          </FormGroup>}
        </Form>
      </React.Fragment>
    );
  }
}
