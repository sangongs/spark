/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution;

import java.io.IOException;
import java.util.LinkedList;

import scala.collection.Iterator;

import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;

/**
 * An iterator interface used to pull the output from generated function for multiple operators
 * (whole stage codegen).
 */
public abstract class BufferedIterator<T> {
  protected LinkedList<T> currentRows = new LinkedList<>();
  private long startTimeNs = System.nanoTime();

  protected int partitionIndex = -1;

  public boolean hasNext() throws IOException {
    if (currentRows.isEmpty()) {
      processNext();
    }
    return !currentRows.isEmpty();
  }

  public T next() {
    return currentRows.remove();
  }

  /**
   * Returns the elapsed time since this object is created. This object represents a pipeline so
   * this is a measure of how long the pipeline has been running.
   */
  public long durationMs() {
    return (System.nanoTime() - startTimeNs) / (1000 * 1000);
  }

  /**
   * Initializes from array of iterators of T.
   */
  public abstract void init(int index, Iterator<InternalRow>[] iters);

  /*
   * Attributes of the following four methods are public. Thus, they can be also accessed from
   * methods in inner classes. See SPARK-23598
   */
  /**
   * Append a row to currentRows.
   */
  public void append(T row) {
    currentRows.add(row);
  }

  /**
   * Returns whether this iterator should stop fetching next row from [[CodegenSupport#inputRDDs]].
   *
   * If it returns true, the caller should exit the loop that [[InputAdapter]] generates.
   * This interface is mainly used to limit the number of input rows.
   */
  public boolean stopEarly() {
    return false;
  }

  /**
   * Returns whether `processNext()` should stop processing next row from `input` or not.
   *
   * If it returns true, the caller should exit the loop (return from processNext()).
   */
  public boolean shouldStop() {
    return !currentRows.isEmpty();
  }

  /**
   * Increase the peak execution memory for current task.
   */
  public void incPeakExecutionMemory(long size) {
    TaskContext.get().taskMetrics().incPeakExecutionMemory(size);
  }

  /**
   * Processes the input until have a row as output (currentRow).
   *
   * After it's called, if currentRow is still null, it means no more rows left.
   */
  protected abstract void processNext() throws IOException;
}
