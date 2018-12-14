export const shuffleArray = (array: any[]) => array
  .map((element: any) => [Math.random(), element])
  .sort((a: any, b: any) => a[0] - b[0])
  .map((element: any) => element[1]);
