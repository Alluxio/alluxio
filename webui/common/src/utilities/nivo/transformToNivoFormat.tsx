export interface TranformableData {
  [key: string]: string | number;
}

export const transformToNivoFormat = (data: TranformableData[], xKeyName: string, yKeyName: string) => {
  data.forEach((item: TranformableData, index: number) => {
    item.x = item[xKeyName];
    item.y = item[yKeyName];
    delete item[xKeyName];
    delete item[yKeyName];
    data[index] = item;
  });
  return data;
};
