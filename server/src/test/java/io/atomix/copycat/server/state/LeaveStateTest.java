package io.atomix.copycat.server.state;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class LeaveStateTest extends AbstractStateTest<LeaveState> {
  @BeforeMethod
  @Override
  void beforeMethod() throws Throwable {
    super.beforeMethod();
    state = new LeaveState(serverState);
  }

}
