package io.atomix.copycat.server.state;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.atomix.copycat.server.CopycatServer.State;


@Test
public class ActiveStateTest extends AbstractStateTest<ActiveState> {
  @BeforeMethod
  @Override
  void beforeMethod() throws Throwable {
    super.beforeMethod();
    state = new ActiveState(serverState) {
    };
  }

  @Override
  public void testAppendRejectedWhenRequestTermIsOld() throws Throwable {
    super.testAppendUpdatesLeaderAndTerm();
    
    assertEquals(serverState.getState(), State.FOLLOWER);
  }
  
  public void testHandlePoll() throws Throwable {
    // TODO
  }
  
  public void testHandleVote() throws Throwable {
    // TODO
  }
  
  public void testIsLogUpToDate() throws Throwable {
    // TODO    
  }
  
  public void testQuery() throws Throwable {
    // TODO
  }
}
