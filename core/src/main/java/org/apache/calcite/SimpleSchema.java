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
public class SimpleSchema {
  public final Employee[] employees = {
      new Employee(10, 1, "Daniel"),
      new Employee(20, 1, "Mark"),
      new Employee(30, 2, "Smith"),
      new Employee(40, 3, "Armstrong"),
      new Employee(50, 2, "Gabriel"),
      new Employee(60, 5, "Daniel"),
      new Employee(70, 7, "Joe"),
      new Employee(80, 2, "Kim"),
      new Employee(90, 1, "Gino")
  };

  public final Department[] departments = {
      new Department(1, "Engineering", 100),
      new Department(2, "Finance", 100)
  };

  public final Company[] companies = {
      new Company(100, "All Link Pte Ltd"),
      new Company(200, "")
  };

  /**
   *  Represents the schema of the employee table. */
  public static class Employee {
    public final int empID;
    public final int depID;
    public final String name;

    Employee(int empID, int depID, String name) {
      this.empID = empID;
      this.depID = depID;
      this.name = name;
    }
  }

  /**
   *  Represents the schema of the department table. */
  public static class Department {
    public final int depID;
    public final String depName;
    public final int cmpID;

    Department(int depID, String depName, int cmpID) {
      this.depID = depID;
      this.depName = depName;
      this.cmpID = cmpID;
    }
  }

  /**
   *  Represents the schema of the company table. */
  public static class Company {
    public final int cmpID;
    public final String cmpName;

    Company(int cmpID, String cmpName) {
      this.cmpID = cmpID;
      this.cmpName = cmpName;
    }
  }
}

// End SimpleSchema.java
