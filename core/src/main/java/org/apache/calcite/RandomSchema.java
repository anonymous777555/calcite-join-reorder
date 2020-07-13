/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite;

/**
 * Represents the database schema used to generate random queries.
 */
public class RandomSchema {
  public final A[] a = {
      new A(1),
      new A(2),
      new A(3),
      new A(4),
      new A(5),
      new A(6)
  };

  public final B[] b = {
      new B(1),
      new B(2),
      new B(3),
      new B(4),
      new B(7),
      new B(8)
  };

  public final C[] c = {
      new C(1),
      new C(2),
      new C(3),
      new C(6),
      new C(7),
      new C(9)
  };

  public final D[] d = {
      new D(1),
      new D(2),
      new D(3),
      new D(6),
      new D(11),
      new D(13)
  };

  public final E[] e = {
      new E(1),
      new E(2),
      new E(3),
      new E(8),
      new E(11),
      new E(19)
  };

  public final F[] f = {
      new F(1),
      new F(4),
      new F(7),
      new F(10),
      new F(11),
      new F(12)
  };

  public final G[] g = {
      new G(2),
      new G(3),
      new G(7),
      new G(18),
      new G(19),
      new G(20)
  };

  public final H[] h = {
      new H(2),
      new H(5),
      new H(9),
      new H(12),
      new H(17),
      new H(19)
  };

  /**
   * Represents the database schema for a table. */
  public static class A {
    public final int aID;

    A(int id) {
      this.aID = id;
    }
  }

  /**
   * Represents the database schema for a table. */
  public static class B {
    public final int bID;

    B(int id) {
      this.bID = id;
    }
  }

  /**
   * Represents the database schema for a table. */
  public static class C {
    public final int cID;

    C(int id) {
      this.cID = id;
    }
  }

  /**
   * Represents the database schema for a table. */
  public static class D {
    public final int dID;

    D(int id) {
      this.dID = id;
    }
  }

  /**
   * Represents the database schema for a table. */
  public static class E {
    public final int eID;

    E(int id) {
      this.eID = id;
    }
  }

  /**
   * Represents the database schema for a table. */
  public static class F {
    public final int fID;

    F(int id) {
      this.fID = id;
    }
  }

  /**
   * Represents the database schema for a table. */
  public static class G {
    public final int gID;

    G(int id) {
      this.gID = id;
    }
  }

  /**
   * Represents the database schema for a table. */
  public static class H {
    public final int hID;

    H(int id) {
      this.hID = id;
    }
  }
}

// End RandomSchema.java
