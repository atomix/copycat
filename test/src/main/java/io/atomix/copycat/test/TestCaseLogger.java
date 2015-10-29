package io.atomix.copycat.test;

import org.slf4j.LoggerFactory;
import org.testng.IClass;
import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;

public class TestCaseLogger implements IInvokedMethodListener {
  private void logInvocation(String state, IInvokedMethod method) {
    if (!method.isTestMethod())
      return;

    ITestNGMethod testMethod = method.getTestMethod();
    IClass clazz = testMethod.getTestClass();
    LoggerFactory.getLogger(state).info("{}.{}", clazz.getRealClass(), testMethod.getMethodName());
  }

  public void beforeInvocation(IInvokedMethod method, ITestResult testResult) {
    logInvocation("BEFORE", method);
  }

  public void afterInvocation(IInvokedMethod method, ITestResult testResult) {
    logInvocation("AFTER", method);
  }
}