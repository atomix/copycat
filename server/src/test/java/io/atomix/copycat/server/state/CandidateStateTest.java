package io.atomix.copycat.server.state;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class CandidateStateTest extends AbstractStateTest<CandidateState> {
  @BeforeMethod
  @Override
  void beforeMethod() throws Throwable {
    super.beforeMethod();
    state = new CandidateState(serverState);
  }

  public void testAppend() throws Throwable {
    // TODO
  }
  
  public void testVote() throws Throwable {
    // TODO
  }
  
  public void testSendVoteRequests() throws Throwable {
    // TODO
  }
}
