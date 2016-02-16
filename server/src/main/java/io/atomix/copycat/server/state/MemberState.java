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
  private int failures;

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

}
