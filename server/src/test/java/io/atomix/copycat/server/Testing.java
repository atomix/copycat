package io.atomix.copycat.server;

import java.util.stream.Stream;

public final class Testing {
  static class UncheckedException extends RuntimeException{
    UncheckedException(Throwable cause) {
      super(cause);
    }
  }
  
  @FunctionalInterface
  public interface ThrowableRunnable {
    void run() throws Throwable;
  }

  public static interface TConsumer1<A> {
    void accept(A a) throws Throwable;
  }

  public static <A> void withArgs(A[] a, TConsumer1<A> consumer) throws Throwable {
    try {
      Stream.of(a).forEach(arg -> uncheck(() -> consumer.accept(arg)));
    } catch (UncheckedException e) {
      throw e.getCause();
    }
  }

  public static void uncheck(ThrowableRunnable runnable) {
    try {
      runnable.run();
    } catch (Throwable e) {
      throw new UncheckedException(e);
    }
  }
}
