export const bytesToString = (bytes: number) => {
  if (bytes === 0) {
    return '0Byte';
  }
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = parseInt('' + Math.floor(Math.log(bytes) / Math.log(1024)), 10);
  return Math.round(bytes / Math.pow(1024, i) * 100) /100 + sizes[i];
};
