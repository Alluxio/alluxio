import React from 'react';
import {connect} from 'react-redux';
import {Button, Form, FormGroup, Input, Label, Table} from 'reactstrap';
import {Dispatch} from 'redux';

import {LoadingMessage} from '@alluxio/common-ui/src/components';
import {IFileInfo} from '../../../constants';
import {IApplicationState} from '../../../store';
import {fetchRequest} from '../../../store/browse/actions';
import {IBrowse} from '../../../store/browse/types';

import './Browse.css';

interface IPropsFromState {
  browse: IBrowse;
  errors: string;
  loading: boolean;
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

interface IBrowseState {
  end: boolean;
  limit: number;
  offset: number;
  path: string;
}

type AllProps = IPropsFromState & IPropsFromDispatch;

class Browse extends React.Component<AllProps, IBrowseState> {
  private readonly pathInput = React.createRef<Input>();

  constructor(props: AllProps) {
    super(props);
    this.rootButtonHandler = this.rootButtonHandler.bind(this);
    this.goButtonHandler = this.goButtonHandler.bind(this);

    this.state = {
      end: false,
      limit: 20,
      offset: 0,
      path: '/'
    }
  }

  public componentWillMount() {
    this.props.fetchRequest();
  }

  public render() {
    const {browse, loading} = this.props;
    const {path} = this.state;

    if (loading) {
      return (
        <LoadingMessage/>
      );
    }

    return (
      <div className="browse-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-12">
              <h5>Browse</h5>
              <div>
                <Form id="browseForm" inline={true}>
                  <FormGroup className="mb-2 mr-sm-2">
                    <Button inverse={true} onClick={this.rootButtonHandler}>Root</Button>
                  </FormGroup>
                  <FormGroup className="mb-2 mr-sm-2">
                    <Label for="browsePath" className="mr-sm-2">Path</Label>
                    <Input type="text" id="browsePath" placeholder="Enter a Path" ref={this.pathInput}
                           value={path}/>
                  </FormGroup>
                  <FormGroup className="mb-2 mr-sm-2">
                    <Button color="primary">Go</Button>
                  </FormGroup>
                </Form>
              </div>
              <Table hover={true}>
                <thead>
                <tr>
                  <th/>
                  <th>File Name</th>
                  <th>Block Size</th>
                  <th>In-Alluxio</th>
                  <th>Mode</th>
                  <th>Owner</th>
                  <th>Group</th>
                  <th>Persistence State</th>
                  <th>Pin</th>
                  <th>Creation Time</th>
                </tr>
                </thead>
                <tbody>
                {this.renderIndexBody(browse.fileInfos)}
                </tbody>
              </Table>
            </div>
          </div>
        </div>
      </div>
    );
  }

  private renderIndexBody(fileInfos: IFileInfo[]) {
    return fileInfos.map((fileInfo: IFileInfo) => (
      <tr key={fileInfo.name}>
        <td><i className={fileInfo.isDirectory ? 'fas fa-folder' : 'fas fa-file'} aria-hidden="true"/></td>
        <td>{fileInfo.name}</td>
        <td>{fileInfo.size}</td>
        <td>{fileInfo.inAlluxio}</td>
        <td>{fileInfo.mode}</td>
        <td>{fileInfo.owner}</td>
        <td>{fileInfo.group}</td>
        <td>{fileInfo.persistenceState}</td>
        <td>{fileInfo.pinned}</td>
        <td>{fileInfo.creationTime}</td>
      </tr>
    ));
  }

  private rootButtonHandler() {
    const newPath = '/';
    this.setPath(newPath);
  }

  private goButtonHandler() {
    const newPath = this.pathInput.current ? '' + this.pathInput.current.props.value : '';
    this.setPath(newPath)
  }

  private setPath(newPath: string) {
    const {path} = this.state;
    if (path !== newPath) {
      this.setState({path});
      this.props.fetchRequest(path);
    }
  }
}

const mapStateToProps = ({browse}: IApplicationState) => ({
  browse: browse.browse,
  errors: browse.errors,
  loading: browse.loading
});

const mapDispatchToProps = (dispatch: Dispatch) => ({
  fetchRequest: (path?: string, offset?: number, limit?: number, end?: boolean) => dispatch(fetchRequest(path, offset, limit, end))
});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(Browse);
