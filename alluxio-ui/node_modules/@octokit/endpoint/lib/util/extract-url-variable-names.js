module.exports = extractUrlVariableName

const urlVariableRegex = /\{[^}]+\}/g
function extractUrlVariableName (url) {
  const matches = url.match(urlVariableRegex)

  if (!matches) {
    return []
  }

  return matches.map(removeNonChars).reduce((a, b) => a.concat(b), [])
}

function removeNonChars (variableName) {
  return variableName.replace(/^\W+|\W+$/g, '').split(/,/)
}
