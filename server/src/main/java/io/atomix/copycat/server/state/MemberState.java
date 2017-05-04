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
 * limitations under the License.
 */
package io.atomix.copycat.server.state;

import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.server.storage.Log;

/**
 * Cluster member state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
final class MemberState {
  private static final int MAX_APPENDS = 2;
  private final ServerMember member;
  private long term;
  private long configIndex;
  private long snapshotIndex;
  private long nextSnapshotIndex;
  private int nextSnapshotOffset;
  private long matchIndex;
  private long nextIndex;
  private long heartbeatTime;
  private long heartbeatStartTime;
  private int appending;
  private boolean appendSucceeded;
  private long appendTime;
  private boolean configuring;
  private boolean installing;
  private int failures;
  private final TimeBuffer timeBuffer = new TimeBuffer(8);

  public MemberState(ServerMember member, ClusterState cluster) {
    this.member = Assert.notNull(member, "member").setCluster(cluster);
  }

  /**
   * Resets the member state.
   */
  void resetState(Log log) {
    nextSnapshotIndex = 0;
    nextSnapshotOffset = 0;
    matchIndex = 0;
    nextIndex = log.lastIndex() + 1;
    heartbeatTime = 0;
    heartbeatStartTime = 0;
    appending = 0;
    timeBuffer.reset();
    configuring = false;
    installing = false;
    appendSucceeded = false;
    failures = 0;
  }

  /**
   * Returns the member.
   *
   * @return The member.
   */
  public ServerMember getMember() {
    return member;
  }

  /**
   * Returns the member term.
   *
   * @return The member term.
   */
  long getConfigTerm() {
    return term;
  }

  /**
   * Sets the member term.
   *
   * @param term The member term.
   * @return The member state.
   */
  MemberState setConfigTerm(long term) {
    this.term = term;
    return this;
  }

  /**
   * Returns the member configuration index.
   *
   * @return The member configuration index.
   */
  long getConfigIndex() {
    return configIndex;
  }

  /**
   * Sets the member configuration index.
   *
   * @param configIndex The member configuration index.
   * @return The member state.
   */
  MemberState setConfigIndex(long configIndex) {
    this.configIndex = configIndex;
    return this;
  }

  /**
   * Returns the member's snapshot index.
   *
   * @return The member's snapshot index.
   */
  long getSnapshotIndex() {
    return snapshotIndex;
  }

  /**
   * Sets the member's snapshot index.
   *
   * @param snapshotIndex The member's snapshot index.
   * @return The member state.
   */
  MemberState setSnapshotIndex(long snapshotIndex) {
    this.snapshotIndex = snapshotIndex;
    return this;
  }

  /**
   * Returns the member's next snapshot index.
   *
   * @return The member's next snapshot index.
   */
  long getNextSnapshotIndex() {
    return nextSnapshotIndex;
  }

  /**
   * Sets the member's next snapshot index.
   *
   * @param nextSnapshotIndex The member's next snapshot index.
   * @return The member state.
   */
  MemberState setNextSnapshotIndex(long nextSnapshotIndex) {
    this.nextSnapshotIndex = nextSnapshotIndex;
    return this;
  }

  /**
   * Returns the member's snapshot offset.
   *
   * @return The member's snapshot offset.
   */
  int getNextSnapshotOffset() {
    return nextSnapshotOffset;
  }

  /**
   * Sets the member's snapshot offset.
   *
   * @param nextSnapshotOffset The member's snapshot offset.
   * @return The member state.
   */
  MemberState setNextSnapshotOffset(int nextSnapshotOffset) {
    this.nextSnapshotOffset = nextSnapshotOffset;
    return this;
  }

  /**
   * Returns the member's match index.
   *
   * @return The member's match index.
   */
  long getMatchIndex() {
    return matchIndex;
  }

  /**
   * Sets the member's match index.
   *
   * @param matchIndex The member's match index.
   * @return The member state.
   */
  MemberState setMatchIndex(long matchIndex) {
    this.matchIndex = Assert.argNot(matchIndex, matchIndex < 0, "matchIndex cannot be less than 0");
    return this;
  }

  /**
   * Returns the member's next index.
   *
   * @return The member's next index.
   */
  long getNextIndex() {
    return nextIndex;
  }

  /**
   * Sets the member's next index.
   *
   * @param nextIndex The member's next index.
   * @return The member state.
   */
  MemberState setNextIndex(long nextIndex) {
    this.nextIndex = Assert.argNot(nextIndex, nextIndex <= 0, "nextIndex cannot be less than or equal to 0");
    return this;
  }

  /**
   * Returns a boolean indicating whether an append request can be sent to the member.
   *
   * @return Indicates whether an append request can be sent to the member.
   */
  boolean canAppend() {
    return appending == 0 || (appendSucceeded && appending < MAX_APPENDS && System.nanoTime() - (timeBuffer.average() / MAX_APPENDS) >= appendTime);
  }

  /**
   * Flags the last append to the member as successful.
   *
   * @return The member state.
   */
  MemberState appendSucceeded() {
    return appendSucceeded(true);
  }

  /**
   * Flags the last append to the member is failed.
   *
   * @return The member state.
   */
  MemberState appendFailed() {
    return appendSucceeded(false);
  }

  /**
   * Sets whether the last append to the member succeeded.
   *
   * @param succeeded Whether the last append to the member succeeded.
   * @return The member state.
   */
  private MemberState appendSucceeded(boolean succeeded) {
    this.appendSucceeded = succeeded;
    return this;
  }

  /**
   * Starts an append request to the member.
   *
   * @return The member state.
   */
  MemberState startAppend() {
    appending++;
    appendTime = System.nanoTime();
    return this;
  }

  /**
   * Completes an append request to the member.
   *
   * @return The member state.
   */
  MemberState completeAppend() {
    appending--;
    return this;
  }

  /**
   * Completes an append request to the member.
   *
   * @param time The time in milliseconds for the append.
   * @return The member state.
   */
  MemberState completeAppend(long time) {
    timeBuffer.record(time);
    return completeAppend();
  }

  /**
   * Returns a boolean indicating whether a configure request can be sent to the member.
   *
   * @return Indicates whether a configure request can be sent to the member.
   */
  boolean canConfigure() {
    return !configuring;
  }

  /**
   * Starts a configure request to the member.
   *
   * @return The member state.
   */
  MemberState startConfigure() {
    configuring = true;
    return this;
  }

  /**
   * Completes a configure request to the member.
   *
   * @return The member state.
   */
  MemberState completeConfigure() {
    configuring = false;
    return this;
  }

  /**
   * Returns a boolean indicating whether an install request can be sent to the member.
   *
   * @return Indicates whether an install request can be sent to the member.
   */
  boolean canInstall() {
    return !installing;
  }

  /**
   * Starts an install request to the member.
   *
   * @return The member state.
   */
  MemberState startInstall() {
    installing = true;
    return this;
  }

  /**
   * Completes an install request to the member.
   *
   * @return The member state.
   */
  MemberState completeInstall() {
    installing = false;
    return this;
  }

  /**
   * Returns the member heartbeat time.
   *
   * @return The member heartbeat time.
   */
  long getHeartbeatTime() {
    return heartbeatTime;
  }

  /**
   * Sets the member heartbeat time.
   *
   * @param heartbeatTime The member heartbeat time.
   * @return The member state.
   */
  MemberState setHeartbeatTime(long heartbeatTime) {
    this.heartbeatTime = heartbeatTime;
    return this;
  }

  /**
   * Returns the member heartbeat start time.
   *
   * @return The member heartbeat start time.
   */
  long getHeartbeatStartTime() {
    return heartbeatStartTime;
  }

  /**
   * Sets the member heartbeat start time.
   *
   * @param startTime The member heartbeat attempt start time.
   * @return The member state.
   */
  MemberState setHeartbeatStartTime(long startTime) {
    this.heartbeatStartTime = startTime;
    return this;
  }

  /**
   * Returns the member failure count.
   *
   * @return The member failure count.
   */
  int getFailureCount() {
    return failures;
  }

  /**
   * Increments the member failure count.
   *
   * @return The member state.
   */
  int incrementFailureCount() {
    return ++failures;
  }

  /**
   * Resets the member failure count.
   *
   * @return The member state.
   */
  MemberState resetFailureCount() {
    failures = 0;
    return this;
  }

  @Override
  public String toString() {
    return member.serverAddress().toString();
  }

  /**
   * Timestamp ring buffer.
   */
  private static class TimeBuffer {
    private final long[] buffer;
    private int position;

    public TimeBuffer(int size) {
      this.buffer = new long[size];
    }

    /**
     * Records a request round trip time.
     *
     * @param time The request round trip time to record.
     */
    public void record(long time) {
      buffer[position++] = time;
      if (position >= buffer.length) {
        position = 0;
      }
    }

    /**
     * Returns the average of all recorded round trip times.
     *
     * @return The average of all recorded round trip times.
     */
    public long average() {
      long total = 0;
      for (long time : buffer) {
        if (time > 0) {
          total += time;
        }
      }
      return total / buffer.length;
    }

    /**
     * Resets the recorded round trip times.
     */
    public void reset() {
      for (int i = 0; i < buffer.length; i++) {
        buffer[i] = 0;
      }
      position = 0;
    }
  }

}
