export const isExternalLink = (url: string | undefined) => !!url && !url.match(/^\/[^/].*/);
