export const isNotEmpty = (name: string, value: string) => {
  if (!name) {
    return 'element name cannot be empty';
  }

  const text = value.trim();
  return !!text ? '' : `"${name}" cannot be empty.`;
};
