module.exports = removeHook

function removeHook (state, kind, name, method) {
  if (kind) {
    console.warn(
      'hook.remove.%s(name, method) is deprecated, use hook.remove(name, method)',
      kind
    )
  }
  if (!state.registry[name]) {
    return
  }

  var index = state.registry[name]
    .map(function (registered) { return registered.orig })
    .indexOf(method)

  if (index === -1) {
    return
  }

  state.registry[name].splice(index, 1)
}
