package io.atomix.copycat.client.util;

import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 * Ordered completable future test.
 */
@Test
public class OrderedCompletableFutureTest {

  /**
   * Tests ordered completion of future callbacks.
   */
  public void testOrderedCompletion() throws Throwable {
    CompletableFuture<String> future = new OrderedCompletableFuture<>();
    AtomicInteger order = new AtomicInteger();
    future.whenComplete((r, e) -> assertEquals(1, order.incrementAndGet()));
    future.whenComplete((r, e) -> assertEquals(2, order.incrementAndGet()));
    future.handle((r, e) -> {
      assertEquals(3, order.incrementAndGet());
      assertEquals(r, "foo");
      return "bar";
    });
    future.thenRun(() -> assertEquals(3, order.incrementAndGet()));
    future.thenAccept(r -> {
      assertEquals(5, order.incrementAndGet());
      assertEquals(r, "foo");
    });
    future.thenApply(r -> {
      assertEquals(6, order.incrementAndGet());
      assertEquals(r, "foo");
      return "bar";
    });
    future.whenComplete((r, e) -> {
      assertEquals(7, order.incrementAndGet());
      assertEquals(r, "foo");
    });
    future.complete("foo");
  }

  /**
   * Tests ordered failure of future callbacks.
   */
  public void testOrderedFailure() throws Throwable {
    CompletableFuture<String> future = new OrderedCompletableFuture<>();
    AtomicInteger order = new AtomicInteger();
    future.whenComplete((r, e) -> assertEquals(1, order.incrementAndGet()));
    future.whenComplete((r, e) -> assertEquals(2, order.incrementAndGet()));
    future.handle((r, e) -> {
      assertEquals(3, order.incrementAndGet());
      return "bar";
    });
    future.thenRun(() -> fail());
    future.thenAccept(r -> fail());
    future.exceptionally(e -> {
      assertEquals(3, order.incrementAndGet());
      return "bar";
    });
    future.completeExceptionally(new RuntimeException("foo"));
  }

  /**
   * Tests calling callbacks that are added after completion.
   */
  public void testAfterComplete() throws Throwable {
    CompletableFuture<String> future = new OrderedCompletableFuture<>();
    future.whenComplete((result, error) -> assertEquals(result, "foo"));
    future.complete("foo");
    AtomicInteger count = new AtomicInteger();
    future.whenComplete((result, error) -> {
      assertEquals(result, "foo");
      assertEquals(count.incrementAndGet(), 1);
    });
    future.thenAccept(result -> {
      assertEquals(result, "foo");
      assertEquals(count.incrementAndGet(), 2);
    });
    assertEquals(count.get(), 2);
  }
}
