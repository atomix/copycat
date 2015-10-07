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
package io.atomix.copycat.server.storage.compaction;

import io.atomix.copycat.server.storage.Segment;
import io.atomix.copycat.server.storage.SegmentManager;
import io.atomix.copycat.server.storage.Storage;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Major compaction manager.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class MajorCompactionManager implements CompactionManager {

  @Override
  public List<CompactionTask> buildTasks(Storage storage, SegmentManager segments) {
    List<Segment> cleanableSegments = getCleanableSegments(segments);
    return !cleanableSegments.isEmpty() ? Collections.singletonList(new MajorCompactionTask(segments, cleanableSegments)) : Collections.EMPTY_LIST;
  }

  /**
   * Returns a list of cleanable log segments.
   *
   * @param manager The segment manager.
   * @return A list of cleanable log segments.
   */
  private List<Segment> getCleanableSegments(SegmentManager manager) {
    return manager.segments().stream().filter(s -> s.lastIndex() <= manager.commitIndex() && s.isFull()).collect(Collectors.toList());
  }

}
