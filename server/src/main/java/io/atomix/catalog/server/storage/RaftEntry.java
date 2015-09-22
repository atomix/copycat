package io.atomix.catalog.server.storage;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.ReferenceManager;

/**
 * Raft entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class RaftEntry<T extends RaftEntry<T>> extends Entry<T> {
  private long id;
  private long term;

  protected RaftEntry() {
    super();
  }

  protected RaftEntry(ReferenceManager<Entry<?>> referenceManager) {
    super(referenceManager);
  }

  @Override
  public long getAddress() {
    return 0;
  }

  @Override
  public long getId() {
    return id;
  }

  /**
   * Sets the entry ID.
   *
   * @param id The entry ID.
   * @return The entry entry.
   */
  @SuppressWarnings("unchecked")
  public T setId(long id) {
    this.id = id;
    return (T) this;
  }

  /**
   * Returns the entry term.
   *
   * @return The entry term.
   */
  public long getTerm() {
    return term;
  }

  /**
   * Sets the entry term.
   *
   * @param term The entry term.
   * @return The entry.
   */
  @SuppressWarnings("unchecked")
  public T setTerm(long term) {
    this.term = term;
    return (T) this;
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    buffer.writeLong(id).writeLong(term);
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    id = buffer.readLong();
    term = buffer.readLong();
  }

}
