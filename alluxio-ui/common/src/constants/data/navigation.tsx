export interface INavigationData {
  attributes?: any;
  innerNavs?: INavigationData[];
  innerText: string;
  url?: string;
}
