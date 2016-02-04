/**
 * Set of utilities for working with retryable operations. The main entrypoint is
 * {@link alluxio.retry.RetryPolicy} which is designed to work with do/while loops.
 * <p>
 * Example
 * </p>
 * <pre>
 * {
 *   &#064;code
 *   RetryPolicy retry = new ExponentialBackoffRetry(50, Constants.SECOND_MS, MAX_CONNECT_TRY);
 *   do {
 *     // work to retry
 *   } while (retry.attemptRetry());
 * }
 * </pre>
 */
package alluxio.retry;

