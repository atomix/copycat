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
package io.atomix.copycat.error;

/**
 * Base Copycat protocol exception.
 * <p>
 * This is the base exception type for all Copycat protocol exceptions. Protocol exceptions must be
 * associated with a {@link CopycatError.Type} which is used for more efficient serialization.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class CopycatException extends RuntimeException {
  private final CopycatError.Type type;

  protected CopycatException(CopycatError.Type type, String message, Object... args) {
    super(String.format(message, args));
    if (type == null)
      throw new NullPointerException("type cannot be null");
    this.type = type;
  }

  protected CopycatException(CopycatError.Type type, Throwable cause, String message, Object... args) {
    super(String.format(message, args), cause);
    if (type == null)
      throw new NullPointerException("type cannot be null");
    this.type = type;
  }

  protected CopycatException(CopycatError.Type type, Throwable cause) {
    super(cause);
    if (type == null)
      throw new NullPointerException("type cannot be null");
    this.type = type;
  }

  /**
   * Returns the exception type.
   *
   * @return The exception type.
   */
  public CopycatError.Type getType() {
    return type;
  }

}
