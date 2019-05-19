interface HookInstance {
  /**
   * Invoke before and after hooks.
   */
  (name: string | string[], method: (options: any) => any): Promise<any>
  /**
   * Invoke before and after hooks.
   */
  (name: string | string[], options: any, method: (options: any) => any): Promise<any>
  /**
   * Add before hook for given name. Returns `hook` instance for chaining.
   */
  before (name: string, method: (options: any) => any): HookInstance
  /**
   * Add error hook for given name. Returns `hook` instance for chaining.
   */
  error (name: string, method: (options: any) => any): HookInstance
  /**
   * Add after hook for given name. Returns `hook` instance for chaining.
   */
  after (name: string, method: (options: any) => any): HookInstance
  /**
   * Add wrap hook for given name. Returns `hook` instance for chaining.
   */
  wrap (name: string, method: (options: any) => any): HookInstance
  /**
   * Removes hook for given name. Returns `hook` instance for chaining.
   */
  remove (name: string, beforeHookMethod: (options: any) => any): HookInstance
}

interface HookType {
  new (): HookInstance
}

declare const Hook: HookType
export = Hook
