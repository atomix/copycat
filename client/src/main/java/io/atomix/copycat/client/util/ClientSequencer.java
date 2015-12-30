/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */
package io.atomix.copycat.client.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Client request-response sequencer.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class ClientSequencer {
  private final Map<Long, Runnable> responseCallbacks = new HashMap<>();
  private long requestSequence;
  private long responseSequence;

  /**
   * Returns the next sequence number in the sequencer.
   *
   * @return The next sequence number.
   */
  public long nextSequence() {
    return ++requestSequence;
  }

  /**
   * Runs the given callback in proper sequential order.
   *
   * @param sequenceNumber The sequence number of the callback to run.
   * @param callback The callback to run in sequential order.
   * @return The sequencer.
   */
  public ClientSequencer sequence(long sequenceNumber, Runnable callback) {
    // If the response is for the next sequence number (the response is received in order),
    // complete the future as appropriate. Note that some prior responses may have been received
    // out of order, so once this response is completed, complete any following responses that
    // are in sequence.
    if (sequenceNumber == responseSequence + 1) {
      responseSequence++;

      callback.run();

      // Iterate through responses in sequence if available and trigger completion callbacks that are in sequence.
      while (responseCallbacks.containsKey(responseSequence + 1)) {
        responseCallbacks.remove(++responseSequence).run();
      }
    } else {
      responseCallbacks.put(sequenceNumber, callback);
    }
    return this;
  }

}
