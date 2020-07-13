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

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.rel.RelCustomRunner;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.custom.AsscomInnerLeftRule;
import org.apache.calcite.rel.rules.custom.AsscomLeftInnerRule;
import org.apache.calcite.rel.rules.custom.AsscomLeftLeftRule;
import org.apache.calcite.rel.rules.custom.AssocFullInnerRule;
import org.apache.calcite.rel.rules.custom.AssocLeftFullRule;
import org.apache.calcite.rel.rules.custom.AssocLeftInnerRule;
import org.apache.calcite.rel.rules.custom.AssocReverseInnerFullRule;
import org.apache.calcite.rel.rules.custom.AssocReverseInnerLeftRule;
import org.apache.calcite.rel.rules.custom.NullifyJoinRule;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;

/**
 * A main driver class to test calcite-core against some simple handwritten queries.
 */
public class SimpleQueries {
  // The default schema and the object to be used.
  private static final String DEFAULT_NAMESPACE = "p";
  private static final Object DEFAULT_SCHEMA_OBJECT = new SimpleSchema();
  private static final Schema DEFAULT_SCHEMA = new ReflectiveSchema(DEFAULT_SCHEMA_OBJECT);

  public static void main(String[] args) throws Exception {
    // Builds the root schema.
    final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    final SchemaPlus schema = rootSchema.add(DEFAULT_NAMESPACE, DEFAULT_SCHEMA);

    // 1. A single inner join.
    String sqlQuery = "select e.name, d.depName "
        + "from p.employees e join p.departments d on e.depID = d.depID";
    Program programs = Programs.ofRules(
        EnumerableRules.ENUMERABLE_PROJECT_RULE,
        EnumerableRules.ENUMERABLE_JOIN_RULE);
    RelCustomRunner.buildAndTransformQuery(programs, schema, sqlQuery, false);

    // 2. A single left outer join.
    sqlQuery = "select e.name, d.depName "
        + "from p.employees e left join p.departments d on e.depID = d.depID AND d.depID >= 1";
    programs = Programs.ofRules(
        NullifyJoinRule.INSTANCE,
        EnumerableRules.ENUMERABLE_PROJECT_RULE,
        EnumerableRules.ENUMERABLE_JOIN_RULE);
    RelCustomRunner.buildAndTransformQuery(programs, schema, sqlQuery, false);

    // 3. Two joins (left outer join + inner join) - for Rule 15.
    sqlQuery = "select e.name, d.depName, c.cmpName "
        + "from p.employees e "
        + "left join p.departments d on e.depID = d.depID "
        + "inner join p.companies c on d.cmpID = c.cmpID";
    programs = Programs.ofRules(
        AssocLeftInnerRule.INSTANCE,
        EnumerableRules.ENUMERABLE_PROJECT_RULE,
        EnumerableRules.ENUMERABLE_JOIN_RULE);
    RelCustomRunner.buildAndTransformQuery(programs, schema, sqlQuery);

    // 4. Two joins (inner join + left outer join) - for Rule 16.
    sqlQuery = "select e.name, d.depName, c.cmpName "
        + "from p.departments d "
        + "inner join p.employees e on d.depID = e.depID "
        + "right join p.companies c on d.cmpID = c.cmpID";
    programs = Programs.ofRules(
        AssocReverseInnerLeftRule.INSTANCE,
        JoinCommuteRule.SWAP_OUTER,
        EnumerableRules.ENUMERABLE_PROJECT_RULE,
        EnumerableRules.ENUMERABLE_JOIN_RULE,
        EnumerableRules.ENUMERABLE_BEST_MATCH_RULE,
        EnumerableRules.ENUMERABLE_NULLIFY_RULE);
    RelCustomRunner.buildAndTransformQuery(programs, schema, sqlQuery);

    // 5. Two joins (left outer join + inner join) - for Rule 17.
    sqlQuery = "select e.name, d.depName, c.cmpName "
        + "from p.employees e "
        + "left join p.departments d on e.depID = d.depID "
        + "inner join p.companies c on d.cmpID = c.cmpID";
    programs = Programs.ofRules(
        AsscomLeftInnerRule.INSTANCE,
        JoinCommuteRule.INSTANCE,
        EnumerableRules.ENUMERABLE_PROJECT_RULE,
        EnumerableRules.ENUMERABLE_JOIN_RULE);
    RelCustomRunner.buildAndTransformQuery(programs, schema, sqlQuery);

    // 6. Two joins (inner join + left outer join) - for Rule 18.
    sqlQuery = "select e.name, d.depName, c.cmpName "
        + "from p.employees e "
        + "inner join p.departments d on e.depID = d.depID "
        + "right join p.companies c on d.cmpID = c.cmpID";
    programs = Programs.ofRules(
        AsscomInnerLeftRule.INSTANCE,
        JoinCommuteRule.SWAP_OUTER,
        EnumerableRules.ENUMERABLE_PROJECT_RULE,
        EnumerableRules.ENUMERABLE_JOIN_RULE,
        EnumerableRules.ENUMERABLE_BEST_MATCH_RULE,
        EnumerableRules.ENUMERABLE_NULLIFY_RULE);
    RelCustomRunner.buildAndTransformQuery(programs, schema, sqlQuery);

    // 7. Two joins (left outer join + left outer join) - for Rule 19.
    sqlQuery = "select e.name, d.depName, c.cmpName "
        + "from p.employees e "
        + "left join p.departments d on e.depID = d.depID "
        + "right join p.companies c on d.cmpID = c.cmpID";
    programs = Programs.ofRules(
        AsscomLeftLeftRule.INSTANCE,
        JoinCommuteRule.SWAP_OUTER,
        EnumerableRules.ENUMERABLE_PROJECT_RULE,
        EnumerableRules.ENUMERABLE_JOIN_RULE,
        EnumerableRules.ENUMERABLE_BEST_MATCH_RULE,
        EnumerableRules.ENUMERABLE_NULLIFY_RULE);
    RelCustomRunner.buildAndTransformQuery(programs, schema, sqlQuery);

    // 8. Two joins (full outer join + inner join) - for Rule 1.
    sqlQuery = "select e.name, d.depName, c.cmpName "
        + "from p.employees e "
        + "full join p.departments d on e.depID = d.depID "
        + "inner join p.companies c on d.cmpID = c.cmpID";
    programs = Programs.ofRules(
        AssocFullInnerRule.INSTANCE,
        EnumerableRules.ENUMERABLE_PROJECT_RULE,
        EnumerableRules.ENUMERABLE_JOIN_RULE);
    RelCustomRunner.buildAndTransformQuery(programs, schema, sqlQuery);

    // 9. Two joins (inner join + full outer join) - for Rule 2.
    sqlQuery = "select e.name, d.depName, c.cmpName "
        + "from p.departments d "
        + "inner join p.employees e on d.depID = e.depID "
        + "full join p.companies c on d.cmpID = c.cmpID";
    programs = Programs.ofRules(
        AssocReverseInnerFullRule.INSTANCE,
        JoinCommuteRule.SWAP_OUTER,
        EnumerableRules.ENUMERABLE_PROJECT_RULE,
        EnumerableRules.ENUMERABLE_JOIN_RULE);
    RelCustomRunner.buildAndTransformQuery(programs, schema, sqlQuery);

    // 10. Two joins (left join + full outer join) - for Rule 6.
    sqlQuery = "select e.name, d.depName, c.cmpName "
        + "from p.employees e "
        + "left join p.departments d on e.depID = d.depID "
        + "full join p.companies c on d.cmpID = c.cmpID";
    programs = Programs.ofRules(
        AssocLeftFullRule.INSTANCE,
        EnumerableRules.ENUMERABLE_PROJECT_RULE,
        EnumerableRules.ENUMERABLE_JOIN_RULE);
    RelCustomRunner.buildAndTransformQuery(programs, schema, sqlQuery);
  }

  private SimpleQueries() {
  }
}

// End SimpleQueries.java
