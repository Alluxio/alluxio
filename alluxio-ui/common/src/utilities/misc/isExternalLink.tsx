export const isExternalLink = (url: string | undefined) => !!url && !/^\/[^/].*/.test(url);
