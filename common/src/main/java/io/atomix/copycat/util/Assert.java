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
package io.atomix.copycat.util;

/**
 * Assertion utilities.
 */
public final class Assert {
  private Assert() {
  }

  /**
   * @throws IllegalArgumentException when {@code expression} is false
   */
  public static void arg(boolean expression, String errorMessageFormat, Object... args) {
    if (!expression)
      throw new IllegalArgumentException(String.format(errorMessageFormat, args));
  }

  /**
   * @throws IllegalArgumentException when {@code expression} is false
   */
  public static <T> T arg(T argument, boolean expression, String errorMessageFormat, Object... args) {
    arg(expression, errorMessageFormat, args);
    return argument;
  }

  /**
   * @throws IllegalArgumentException when {@code expression} is true
   */
  public static void argNot(boolean expression, String errorMessageFormat, Object... args) {
    arg(!expression, errorMessageFormat, args);
  }

  /**
   * @throws IllegalArgumentException when {@code expression} is true
   */
  public static <T> T argNot(T argument, boolean expression, String errorMessageFormat, Object... args) {
    return arg(argument, !expression, errorMessageFormat, args);
  }

  /**
   * @throws IndexOutOfBoundsException when {@code expression} is false
   */
  public static void index(boolean expression, String errorMessageFormat, Object... args) {
    if (!expression)
      throw new IndexOutOfBoundsException(String.format(errorMessageFormat, args));
  }

  /**
   * @throws IndexOutOfBoundsException when {@code expression} is false
   */
  public static void indexNot(boolean expression, String errorMessageFormat, Object... args) {
    index(!expression, errorMessageFormat, args);
  }

  /**
   * @throws NullPointerException when {@code reference} is null
   */
  public static <T> T notNull(T reference, String parameterName) {
    if (reference == null)
      throw new NullPointerException(parameterName + " cannot be null");
    return reference;
  }

  /**
   * @throws IllegalStateException when {@code expression} is false
   */
  public static void state(boolean expression, String errorMessageFormat, Object... args) {
    if (!expression)
      throw new IllegalStateException(String.format(errorMessageFormat, args));
  }

  /**
   * @throws IllegalStateException when {@code expression} is true
   */
  public static void stateNot(boolean expression, String errorMessageFormat, Object... args) {
    state(!expression, errorMessageFormat, args);
  }
}
