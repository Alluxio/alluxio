import React from 'react';
import {connect} from 'react-redux';
import {Link} from 'react-router-dom';
import {Alert, Table} from 'reactstrap';
import {Dispatch} from 'redux';

import {IFileBlockInfo, IFileInfo} from '@alluxio/common-ui/src/constants';
import {parseQuerystring} from '@alluxio/common-ui/src/utilities';
import {IApplicationState} from '../../../store';
import {fetchRequest} from '../../../store/blockInfo/actions';
import {IBlockInfo} from '../../../store/blockInfo/types';

interface IPropsFromState {
  blockInfo: IBlockInfo;
  errors: string;
  loading: boolean;
  location: {
    search: string;
  };
}

interface IPropsFromDispatch {
  fetchRequest: typeof fetchRequest;
}

interface IBlockInfoProps {
  refreshValue: boolean;
}

interface IBlockInfoState {
  end?: string;
  limit?: string;
  offset?: string;
  path?: string;
  lastFetched: {
    end?: string;
    limit?: string;
    offset?: string;
    path?: string;
  };
}

type AllProps = IPropsFromState & IPropsFromDispatch & IBlockInfoProps;

class BlockInfo extends React.Component<AllProps, IBlockInfoState> {
  constructor(props: AllProps) {
    super(props);

    const {path, offset, limit, end} = parseQuerystring(this.props.location.search);
    this.state = {end, limit, offset, path: path || '/', lastFetched: {}};
  }

  public componentWillReceiveProps(props: AllProps) {
    const {refreshValue} = this.props;
    if (props.refreshValue !== refreshValue) {
      const {path, offset, limit, end} = this.state;
      this.fetchData(path, offset, limit, end);
    }
  }

  public componentDidUpdate(prevProps: AllProps) {
    if (this.props.location.search !== prevProps.location.search) {
      const {path, offset, limit, end} = parseQuerystring(this.props.location.search);
      this.setState({path, offset, limit, end});
      this.fetchData(path, offset, limit, end);
    }
  }

  public componentWillMount() {
    const {path, offset, limit, end} = this.state;
    this.fetchData(path, offset, limit, end);
  }

  public render() {
    const {errors, blockInfo} = this.props;
    let queryStringSuffix = ['offset', 'limit', 'end'].filter((key: string) => this.state[key] !== undefined)
      .map((key: string) => `${key}=${this.state[key]}`).join('&');
    queryStringSuffix = queryStringSuffix ? '&' + queryStringSuffix : queryStringSuffix;

    if (errors || blockInfo.invalidPathError || blockInfo.fatalError) {
      return (
        <Alert color="danger">
          {errors && <div>Unable to reach the api endpoint for this page.</div>}
          {blockInfo.invalidPathError && <div>{blockInfo.invalidPathError}</div>}
          {blockInfo.fatalError && <div>{blockInfo.fatalError}</div>}
        </Alert>
      );
    }

    return (
      <div className="blockInfo-page">
        <div className="container-fluid">
          <div className="row">
            <div className="col-12">
              {blockInfo.blockSizeBytes && this.renderBlockInfoView(blockInfo)}
              {!blockInfo.blockSizeBytes && this.renderBlockInfoListing(blockInfo.fileInfos, blockInfo.orderedTierAliases, queryStringSuffix)}
            </div>
          </div>
        </div>
      </div>
    );
  }

  private renderBlockInfoView(blockInfo: IBlockInfo) {
    const {path} = this.state;
    return (
      <React.Fragment>
        <h5>{path}</h5>
        <hr/>
        <h6>Blocks on this worker (block capacity is {blockInfo.blockSizeBytes} Bytes):</h6>
        <Table hover={true}>
          <thead>
          <tr>
            <th>ID</th>
            <th>Tier</th>
            <th>Size (Byte)</th>
          </tr>
          </thead>
          <tbody>
            {Object.keys(blockInfo.fileBlocksOnTier).map((index: string) => {
              const fileBlocksOnTier = blockInfo.fileBlocksOnTier[index];
              return Object.keys(fileBlocksOnTier).map((tierAlias: string) => {
                const fileBlocksDatas: IFileBlockInfo[] = fileBlocksOnTier[tierAlias];
                return fileBlocksDatas.map((fileBlocksData: IFileBlockInfo) => (
                  <tr key={index + tierAlias}>
                    <td>{fileBlocksData.id}</td>
                    <td>{tierAlias}</td>
                    <td>{fileBlocksData.blockLength}</td>
                  </tr>
                ));
              });
            })}
          </tbody>
        </Table>
      </React.Fragment>
    );
  }

  private renderBlockInfoListing(fileInfos: IFileInfo[], tierAliases: string[], queryStringSuffix: string) {
    return (
      <React.Fragment>
        <Table hover={true}>
          <thead>
          <tr>
            <th>File Path</th>
            {tierAliases.map((tierAlias: string) => (
              <th key={tierAlias}>in-{tierAlias}</th>
            ))}
            <th>Size</th>
            <th>Creation Time</th>
            <th>Modification Time</th>
          </tr>
          </thead>
          <tbody>
          {fileInfos.map((fileInfo: IFileInfo) => (
            <tr key={fileInfo.absolutePath}>
              <td>{this.renderFileNameLink(fileInfo.absolutePath, queryStringSuffix)}</td>
              {tierAliases.map((tierAlias: string) => (
                // TODO: figure out how to get this
                <td key={tierAlias}/>
              ))}
              <td>{fileInfo.size}</td>
              <td>{fileInfo.creationTime}</td>
              <td>{fileInfo.modificationTime}</td>
            </tr>
          ))}
          </tbody>
        </Table>
      </React.Fragment>
    )
  }

  private renderFileNameLink(filePath: string, queryStringSuffix: string) {
    const {lastFetched} = this.state;
    if (filePath === lastFetched.path) {
      return (
        filePath
      );
    }

    return (
      <Link to={`/blockInfo?path=${filePath}${queryStringSuffix}`}>
        {filePath}
      </Link>
    );
  }

  private fetchData(path?: string, offset?: string, limit?: string, end?: string) {
    this.setState({lastFetched: {path, offset, limit, end}});
    this.props.fetchRequest(path, offset, limit, end);
  }
}

const mapStateToProps = ({blockInfo}: IApplicationState) => ({
  blockInfo: blockInfo.blockInfo,
  errors: blockInfo.errors,
  loading: blockInfo.loading
});

const mapDispatchToProps = (dispatch: Dispatch) => ({
  fetchRequest: (path?: string, offset?: string, limit?: string, end?: string) => dispatch(fetchRequest(path, offset, limit, end))
});

export default connect(
  mapStateToProps,
  mapDispatchToProps
)(BlockInfo);
