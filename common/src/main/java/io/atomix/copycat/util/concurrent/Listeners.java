/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.copycat.util.concurrent;

import io.atomix.copycat.util.Assert;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;

/**
 * Utility for managing a set of listeners.
 *
 * @param <T> event type
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Listeners<T> implements Iterable<Listener<T>> {
  private final List<ListenerHolder> listeners = new CopyOnWriteArrayList<>();

  /**
   * Returns the number of registered listeners.
   *
   * @return The number of registered listeners.
   */
  public int size() {
    return listeners.size();
  }

  /**
   * Adds a listener to the set of listeners.
   *
   * @param listener The listener to add.
   * @return The listener context.
   * @throws NullPointerException if {@code listener} is null
   */
  public Listener<T> add(Consumer<T> listener) {
    Assert.notNull(listener, "listener");
    ListenerHolder holder = new ListenerHolder(listener, ThreadContext.currentContext());
    listeners.add(holder);
    return holder;
  }

  /**
   * Applies an event to all listeners.
   *
   * @param event The event to apply.
   * @return A completable future to be completed once all listeners have been completed.
   */
  public CompletableFuture<Void> accept(T event) {
    List<CompletableFuture<Void>> futures = new ArrayList<>(listeners.size());
    for (ListenerHolder listener : listeners) {
      if (listener.context != null) {
        futures.add(listener.context.execute(() -> listener.listener.accept(event)));
      } else {
        listener.listener.accept(event);
      }
    }
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterator<Listener<T>> iterator() {
    return (Iterator) listeners.iterator();
  }

  /**
   * Listener holder.
   */
  private class ListenerHolder implements Listener<T> {
    private final Consumer<T> listener;
    private final ThreadContext context;

    private ListenerHolder(Consumer<T> listener, ThreadContext context) {
      this.listener = listener;
      this.context = context;
    }

    @Override
    public void accept(T event) {
      if (context != null) {
        try {
          context.executor().execute(() -> listener.accept(event));
        } catch (RejectedExecutionException e) {
        }
      } else {
        listener.accept(event);
      }
    }

    @Override
    public void close() {
      listeners.remove(this);
    }
  }

}
