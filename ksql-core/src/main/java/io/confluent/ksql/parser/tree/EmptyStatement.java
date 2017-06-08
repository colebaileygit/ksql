/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.parser.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class EmptyStatement extends Statement {

  public EmptyStatement() {
    this(Optional.empty());
  }

  public EmptyStatement(NodeLocation location) {
    this(Optional.of(location));
  }

  private EmptyStatement(Optional<NodeLocation> location) {
    super(location);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return null;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof EmptyStatement;
  }

  @Override
  public String toString() {
    return toStringHelper(this).toString();
  }
}
