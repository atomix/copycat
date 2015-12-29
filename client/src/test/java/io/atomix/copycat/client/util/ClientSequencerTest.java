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

import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Client sequencer test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class ClientSequencerTest {

  /**
   * Tests sequencing callbacks with the sequencer.
   */
  public void testSequence() throws Throwable {
    ClientSequencer sequencer = new ClientSequencer();
    long sequence1 = sequencer.nextSequence();
    long sequence2 = sequencer.nextSequence();
    assertTrue(sequence2 == sequence1 + 1);

    AtomicBoolean run = new AtomicBoolean();
    sequencer.sequence(sequence2, () -> run.set(true));
    sequencer.sequence(sequence1, () -> assertFalse(run.get()));
    assertTrue(run.get());
  }

}
