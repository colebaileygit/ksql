/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.query;

import static io.confluent.ksql.util.KeyValue.keyValue;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KeyValue;
import java.util.Collection;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * A queue of rows for transient queries.
 */
public class TransientQueryQueue implements BlockingRowQueue {

  public static final int BLOCKING_QUEUE_CAPACITY = 500;

  private final BlockingQueue<KeyValue<List<?>, GenericRow>> rowQueue;
  private final int offerTimeoutMs;
  private volatile boolean closed = false;
  private final AtomicInteger remaining;
  private LimitHandler limitHandler;
  private CompletionHandler completionHandler;
  private Runnable queuedCallback;

  public TransientQueryQueue(final OptionalInt limit) {
    this(limit, BLOCKING_QUEUE_CAPACITY, 100);
  }

  @VisibleForTesting
  public TransientQueryQueue(
      final OptionalInt limit,
      final int queueSizeLimit,
      final int offerTimeoutMs
  ) {
    this.remaining = limit.isPresent() ? new AtomicInteger(limit.getAsInt()) : null;
    this.rowQueue = new LinkedBlockingQueue<>(queueSizeLimit);
    this.offerTimeoutMs = offerTimeoutMs;
  }

  @Override
  public void setLimitHandler(final LimitHandler limitHandler) {
    this.limitHandler = limitHandler;
  }

  @Override
  public void setCompletionHandler(final CompletionHandler completionHandler) {
    this.completionHandler = completionHandler;
  }

  @Override
  public void setQueuedCallback(final Runnable queuedCallback) {
    this.queuedCallback = queuedCallback;
  }

  @Override
  public KeyValue<List<?>, GenericRow> poll(final long timeout, final TimeUnit unit)
      throws InterruptedException {
    return rowQueue.poll(timeout, unit);
  }

  @Override
  public KeyValue<List<?>, GenericRow> poll() {
    return rowQueue.poll();
  }

  @Override
  public void drainTo(final Collection<? super KeyValue<List<?>, GenericRow>> collection) {
    rowQueue.drainTo(collection);
  }

  @Override
  public int size() {
    return rowQueue.size();
  }

  @Override
  public boolean isEmpty() {
    return rowQueue.isEmpty();
  }

  @Override
  public void close() {
    closed = true;
  }

  public void acceptRow(final List<?> key, final GenericRow value) {
    try {
      if (checkLimit()) {
        return;
      }

      final KeyValue<List<?>, GenericRow> row = keyValue(key, value);

      while (!closed) {
        if (rowQueue.offer(row, offerTimeoutMs, TimeUnit.MILLISECONDS)) {
          if (updateAndCheckLimit()) {
            limitHandler.limitReached();
          }
          if (queuedCallback != null) {
            queuedCallback.run();
          }
          break;
        }
      }
    } catch (final InterruptedException e) {
      // Forced shutdown?
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Accepts the rows without blocking.
   * @param key The key
   * @param value The value
   * @return If the row was accepted or discarded for an acceptable reason, false if it was rejected
   *     because the queue was full.
   */
  public boolean acceptRowNonBlocking(final List<?> key, final GenericRow value) {
    try {
      if (checkLimit()) {
        return true;
      }

      final KeyValue<List<?>, GenericRow> row = keyValue(key, value);

      if (!closed) {
        if (!rowQueue.offer(row, 0, TimeUnit.MILLISECONDS)) {
          return false;
        }
        if (updateAndCheckLimit()) {
          limitHandler.limitReached();
        }
        queuedCallback.run();
        return true;
      }
    } catch (final InterruptedException e) {
      // Forced shutdown?
      Thread.currentThread().interrupt();
      return false;
    }
    return true;
  }

  public boolean isClosed() {
    return closed;
  }

  private boolean updateAndCheckLimit() {
    return remaining != null && remaining.decrementAndGet() <= 0;
  }

  private boolean checkLimit() {
    return remaining != null && remaining.get() <= 0;
  }

  public void complete() {
    if (completionHandler != null) {
      completionHandler.complete();
    }
  }
}
