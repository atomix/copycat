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
package io.atomix.copycat.server.cluster;

import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.server.storage.Log;

/**
 * Cluster member state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class MemberContext {
  private Member member;
  private long term;
  private long version;
  private long matchIndex;
  private long nextIndex;
  private long commitTime;
  private long commitStartTime;
  private int failures;

  public MemberContext(Member member) {
    this.member = Assert.notNull(member, "member");
  }

  /**
   * Resets the member state.
   */
  public void resetState(Log log) {
    matchIndex = 0;
    nextIndex = log.lastIndex() + 1;
    commitTime = 0;
    commitStartTime = 0;
    failures = 0;
  }

  /**
   * Returs the member.
   *
   * @return The member.
   */
  public Member getMember() {
    return member;
  }

  /**
   * Sets the member.
   *
   * @param member The member.
   * @return The member state.
   */
  public MemberContext setMember(Member member) {
    this.member = Assert.notNull(member, "member");
    return this;
  }

  /**
   * Returns the member term.
   *
   * @return The member term.
   */
  public long getTerm() {
    return term;
  }

  /**
   * Sets the member term.
   *
   * @param term The member term.
   * @return The member state.
   */
  public MemberContext setTerm(long term) {
    this.term = term;
    return this;
  }

  /**
   * Returns the member configuration version.
   *
   * @return The member configuration version.
   */
  public long getVersion() {
    return version;
  }

  /**
   * Sets the member version.
   *
   * @param version The member version.
   * @return The member state.
   */
  public MemberContext setVersion(long version) {
    this.version = version;
    return this;
  }

  /**
   * Returns the member's match index.
   *
   * @return The member's match index.
   */
  public long getMatchIndex() {
    return matchIndex;
  }

  /**
   * Sets the member's match index.
   *
   * @param matchIndex The member's match index.
   * @return The member state.
   */
  public MemberContext setMatchIndex(long matchIndex) {
    this.matchIndex = Assert.argNot(matchIndex, matchIndex < this.matchIndex, "matchIndex cannot be decreased");
    return this;
  }

  /**
   * Returns the member's next index.
   *
   * @return The member's next index.
   */
  public long getNextIndex() {
    return nextIndex;
  }

  /**
   * Sets the member's next index.
   *
   * @param nextIndex The member's next index.
   * @return The member state.
   */
  public MemberContext setNextIndex(long nextIndex) {
    this.nextIndex = Assert.argNot(nextIndex, nextIndex <= matchIndex, "nextIndex cannot be less than or equal to matchIndex");
    return this;
  }

  /**
   * Returns the member commit time.
   *
   * @return The member commit time.
   */
  public long getCommitTime() {
    return commitTime;
  }

  /**
   * Sets the member commit time.
   *
   * @param commitTime The member commit time.
   * @return The member state.
   */
  public MemberContext setCommitTime(long commitTime) {
    this.commitTime = commitTime;
    return this;
  }

  /**
   * Returns the member commit start time.
   *
   * @return The member commit start time.
   */
  public long getCommitStartTime() {
    return commitStartTime;
  }

  /**
   * Sets the member commit start time.
   *
   * @param startTime The member commit attempt start time.
   * @return The member state.
   */
  public MemberContext setCommitStartTime(long startTime) {
    this.commitStartTime = startTime;
    return this;
  }

  /**
   * Returns the member failure count.
   *
   * @return The member failure count.
   */
  public int getFailureCount() {
    return failures;
  }

  /**
   * Increments the member failure count.
   *
   * @return The member state.
   */
  public int incrementFailureCount() {
    return ++failures;
  }

  /**
   * Resets the member failure count.
   *
   * @return The member state.
   */
  public MemberContext resetFailureCount() {
    failures = 0;
    return this;
  }

  @Override
  public String toString() {
    return member.serverAddress().toString();
  }

}
