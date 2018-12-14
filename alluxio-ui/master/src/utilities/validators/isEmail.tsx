export const isEmail = (name: string, value: string) => {
  if (!name) {
    return 'element.name cannot be empty';
  }

  const text = value.trim();
  const emailRex = /^(([^<>()[\]\\.,;:\s@"]+(\.[^<>()[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;
  return emailRex.test(text) ? '' : `"${name}" is not a valid email format.`;
};
