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

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.server.storage.Log;

/**
 * Cluster member state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class MemberState {
  private static final long HEARTBEAT_TIMEOUT = 60000;
  private final Address serverAddress;
  private Address clientAddress;
  private int index;
  private long heartbeatTime;
  private Status status = Status.UNAVAILABLE;
  private long matchIndex;
  private long nextIndex;
  private long commitTime;
  private long commitStartTime;
  private int failures;

  public MemberState(Address serverAddress) {
    this.serverAddress = Assert.notNull(serverAddress, "serverAddress");
  }

  /**
   * Member status.
   */
  enum Status {
    UNAVAILABLE,
    AVAILABLE
  }

  /**
   * Resets the member state.
   */
  void resetState(Log log) {
    matchIndex = 0;
    nextIndex = log.lastIndex() + 1;
    commitTime = 0;
    commitStartTime = 0;
    failures = 0;
  }

  /**
   * Returns the server address.
   *
   * @return The server address.
   */
  public Address getServerAddress() {
    return serverAddress;
  }

  /**
   * Returns the client address.
   *
   * @return The client address.
   */
  public Address getClientAddress() {
    return clientAddress;
  }

  /**
   * Sets the client address.
   *
   * @param address The client address.
   * @return The member state.
   */
  public MemberState setClientAddress(Address address) {
    this.clientAddress = clientAddress;
    return this;
  }

  /**
   * Returns the member index.
   *
   * @return The member index.
   */
  public int getIndex() {
    return index;
  }

  /**
   * Sets the member index.
   *
   * @param index The member index.
   * @return The member state.
   */
  MemberState setIndex(int index) {
    this.index = index;
    return this;
  }

  /**
   * Returns the heartbeat time.
   *
   * @return The heartbeat time.
   */
  public long getHeartbeatTime() {
    return heartbeatTime;
  }

  /**
   * Sets the heartbeat time.
   *
   * @param heartbeatTime The heartbeat time.
   * @return The member state.
   */
  MemberState setHeartbeatTime(long heartbeatTime) {
    this.heartbeatTime = Math.max(this.heartbeatTime, heartbeatTime);
    return this;
  }

  /**
   * Returns the member heartbeat timeout.
   *
   * @return The member heartbeat timeout.
   */
  public long getHeartbeatTimeout() {
    return HEARTBEAT_TIMEOUT;
  }

  /**
   * Returns the member availability status.
   *
   * @return The member availability status.
   */
  Status getStatus() {
    return status;
  }

  /**
   * Sets the member availability status.
   *
   * @param status The member availability status.
   * @return The member state.
   */
  MemberState setStatus(Status status) {
    this.status = Assert.notNull(status, "status");
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
    this.matchIndex = Assert.argNot(matchIndex, matchIndex < this.matchIndex, "matchIndex cannot be decreased");
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
    this.nextIndex = Assert.argNot(nextIndex, nextIndex <= matchIndex, "nextIndex cannot be less than or equal to matchIndex");
    return this;
  }

  /**
   * Returns the member commit time.
   *
   * @return The member commit time.
   */
  long getCommitTime() {
    return commitTime;
  }

  /**
   * Sets the member commit time.
   *
   * @param commitTime The member commit time.
   * @return The member state.
   */
  MemberState setCommitTime(long commitTime) {
    this.commitTime = commitTime;
    return this;
  }

  /**
   * Returns the member commit start time.
   *
   * @return The member commit start time.
   */
  long getCommitStartTime() {
    return commitStartTime;
  }

  /**
   * Sets the member commit start time.
   *
   * @param startTime The member commit attempt start time.
   * @return The member state.
   */
  MemberState setCommitStartTime(long startTime) {
    this.commitStartTime = startTime;
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
    return serverAddress.toString();
  }

}
