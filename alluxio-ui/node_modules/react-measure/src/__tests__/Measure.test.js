import React, { createRef } from 'react'
import { render } from 'react-dom'

describe('Measure', () => {
  let container, resizeObserver, defaultChildrenFn, MeasureWith

  beforeEach(() => {
    jest.useFakeTimers()
    jest.resetModules()

    global.ResizeObserver = jest.fn(
      callback =>
        (resizeObserver = {
          observe: jest.fn(),
          unobserve: jest.fn(),
          disconnect: jest.fn(),
          callback,
        })
    )

    global.requestAnimationFrame = cb => setTimeout(cb, 0)

    defaultChildrenFn = jest.fn(({ measureRef, ...rest }) => (
      <div ref={measureRef}>{JSON.stringify(rest, null, 2)}</div>
    ))
    const setupMeasure = Measure => ({
      children = defaultChildrenFn,
      ...rest
    }) => <Measure {...rest}>{children}</Measure>

    MeasureWith = setupMeasure(require('../Measure').default)
    container = document.createElement('div')
  })

  it('should handel entry', () => {
    const ref = createRef()
    render(<MeasureWith innerRef={ref} />, container)
    expect(container.firstChild).toMatchSnapshot()

    resizeObserver.callback([
      { contentRect: { width: 0, height: 0 }, target: ref.current },
    ])
    jest.runAllTimers()

    expect(defaultChildrenFn).toHaveBeenCalledTimes(2)
    expect(container.firstChild).toMatchSnapshot()
  })

  it('should handel bounds', () => {
    render(<MeasureWith bounds />, container)
    expect(container.firstChild).toMatchSnapshot()

    resizeObserver.callback()
    jest.runAllTimers()

    expect(defaultChildrenFn).toHaveBeenCalledTimes(2)
    expect(container.firstChild).toMatchSnapshot()
  })

  describe('resizeObserver', () => {
    it('should trigger observe when measureRef is attached', () => {
      render(<MeasureWith />, container)
      expect(resizeObserver.observe).toHaveBeenCalledTimes(1)
    })
    it('should always un observer before observing next one', () => {
      const ref = createRef()

      render(<MeasureWith innerRef={ref} children={() => null} />, container)
      expect(ref.current).toBe(null)

      render(
        <MeasureWith
          innerRef={ref}
          children={({ measureRef }) => (
            <>
              <div id={'child1'} ref={measureRef} />
              <div id={'child2'} ref={measureRef} />
            </>
          )}
        />,
        container
      )
      expect(resizeObserver.observe).toHaveBeenCalledTimes(2)
      expect(resizeObserver.unobserve).toHaveBeenCalledTimes(1)

      expect(resizeObserver.observe.mock.calls[0][0].id).toBe('child1')
      expect(resizeObserver.unobserve.mock.calls[0][0].id).toBe('child1')
      expect(resizeObserver.observe.mock.calls[1][0].id).toBe('child2')

      expect(ref.current.id).toBe('child2')
    })
    it('should trigger onResize when resizeObserver callback is called', () => {
      const onResize = jest.fn()

      render(<MeasureWith onResize={onResize} />, container)
      resizeObserver.callback()
      jest.runAllTimers()

      expect(onResize).toHaveBeenCalledTimes(1)
    })
  })
})
