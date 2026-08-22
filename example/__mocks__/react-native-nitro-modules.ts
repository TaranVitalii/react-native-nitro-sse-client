// Jest can't install the real Nitro JSI runtime (it requires the native binary), so
// `NitroModules.createHybridObject` returns a plain object here instead of a real HybridObject.
// Auto-mocked by Jest because this file lives in a root-level __mocks__ dir next to node_modules.
export const NitroModules = {
  createHybridObject: () => ({}),
}
