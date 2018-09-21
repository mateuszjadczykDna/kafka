/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.acl;

import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.junit.Test;

import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.PatternType.PREFIXED;
import static org.apache.kafka.common.resource.ResourceType.ANY;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.common.resource.ResourceType.UNKNOWN;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ResourcePatternFilterTest {
    @Test
    public void shouldBeUnknownIfResourceTypeUnknown() {
        assertTrue(new ResourcePatternFilter(UNKNOWN, null, PatternType.LITERAL).isUnknown());
    }

    @Test
    public void shouldBeUnknownIfPatternTypeUnknown() {
        assertTrue(new ResourcePatternFilter(GROUP, null, PatternType.UNKNOWN).isUnknown());
    }

    @Test
    public void shouldNotMatchIfDifferentResourceType() {
        assertFalse(new ResourcePatternFilter(TOPIC, "Name", LITERAL)
            .matches(new ResourcePattern(GROUP, "Name", LITERAL)));
    }

    @Test
    public void shouldNotMatchIfDifferentName() {
        assertFalse(new ResourcePatternFilter(TOPIC, "Different", PREFIXED)
            .matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));
    }

    @Test
    public void shouldNotMatchIfDifferentNameCase() {
        assertFalse(new ResourcePatternFilter(TOPIC, "NAME", LITERAL)
            .matches(new ResourcePattern(TOPIC, "Name", LITERAL)));
    }

    @Test
    public void shouldNotMatchIfDifferentPatternType() {
        assertFalse(new ResourcePatternFilter(TOPIC, "Name", LITERAL)
            .matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));
    }

    @Test
    public void shouldMatchWhereResourceTypeIsAny() {
        assertTrue(new ResourcePatternFilter(ANY, "Name", PREFIXED)
            .matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));
    }

    @Test
    public void shouldMatchWhereResourceNameIsAny() {
        assertTrue(new ResourcePatternFilter(TOPIC, null, PREFIXED)
            .matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));
    }

    @Test
    public void shouldMatchWherePatternTypeIsAny() {
        assertTrue(new ResourcePatternFilter(TOPIC, null, PatternType.ANY)
            .matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));
    }

    @Test
    public void shouldMatchWherePatternTypeIsMatch() {
        assertTrue(new ResourcePatternFilter(TOPIC, null, PatternType.MATCH)
            .matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));
    }

    @Test
    public void shouldMatchLiteralIfExactMatch() {
        assertTrue(new ResourcePatternFilter(TOPIC, "Name", LITERAL)
            .matches(new ResourcePattern(TOPIC, "Name", LITERAL)));
    }

    @Test
    public void shouldMatchLiteralIfNameMatchesAndFilterIsOnPatternTypeAny() {
        assertTrue(new ResourcePatternFilter(TOPIC, "Name", PatternType.ANY)
            .matches(new ResourcePattern(TOPIC, "Name", LITERAL)));
    }

    @Test
    public void shouldMatchLiteralIfNameMatchesAndFilterIsOnPatternTypeMatch() {
        assertTrue(new ResourcePatternFilter(TOPIC, "Name", PatternType.MATCH)
            .matches(new ResourcePattern(TOPIC, "Name", LITERAL)));
    }

    @Test
    public void shouldNotMatchLiteralIfNamePrefixed() {
        assertFalse(new ResourcePatternFilter(TOPIC, "Name-something", PatternType.MATCH)
            .matches(new ResourcePattern(TOPIC, "Name", LITERAL)));
    }

    @Test
    public void shouldMatchLiteralWildcardIfExactMatch() {
        assertTrue(new ResourcePatternFilter(TOPIC, "*", LITERAL)
            .matches(new ResourcePattern(TOPIC, "*", LITERAL)));
    }

    @Test
    public void shouldNotMatchLiteralWildcardAgainstOtherName() {
        assertFalse(new ResourcePatternFilter(TOPIC, "Name", LITERAL)
            .matches(new ResourcePattern(TOPIC, "*", LITERAL)));
    }

    @Test
    public void shouldNotMatchLiteralWildcardTheWayAround() {
        assertFalse(new ResourcePatternFilter(TOPIC, "*", LITERAL)
            .matches(new ResourcePattern(TOPIC, "Name", LITERAL)));
    }

    @Test
    public void shouldNotMatchLiteralWildcardIfFilterHasPatternTypeOfAny() {
        assertFalse(new ResourcePatternFilter(TOPIC, "Name", PatternType.ANY)
            .matches(new ResourcePattern(TOPIC, "*", LITERAL)));
    }

    @Test
    public void shouldMatchLiteralWildcardIfFilterHasPatternTypeOfMatch() {
        assertTrue(new ResourcePatternFilter(TOPIC, "Name", PatternType.MATCH)
            .matches(new ResourcePattern(TOPIC, "*", LITERAL)));
    }

    @Test
    public void shouldMatchPrefixedIfExactMatch() {
        assertTrue(new ResourcePatternFilter(TOPIC, "Name", PREFIXED)
            .matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));
    }

    @Test
    public void shouldNotMatchIfBothPrefixedAndFilterIsPrefixOfResource() {
        assertFalse(new ResourcePatternFilter(TOPIC, "Name", PREFIXED)
            .matches(new ResourcePattern(TOPIC, "Name-something", PREFIXED)));
    }

    @Test
    public void shouldNotMatchIfBothPrefixedAndResourceIsPrefixOfFilter() {
        assertFalse(new ResourcePatternFilter(TOPIC, "Name-something", PREFIXED)
            .matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));
    }

    @Test
    public void shouldNotMatchPrefixedIfNamePrefixedAnyFilterTypeIsAny() {
        assertFalse(new ResourcePatternFilter(TOPIC, "Name-something", PatternType.ANY)
            .matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));
    }

    @Test
    public void shouldMatchPrefixedIfNamePrefixedAnyFilterTypeIsMatch() {
        assertTrue(new ResourcePatternFilter(TOPIC, "Name-something", PatternType.MATCH)
            .matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));
    }

    @Test
    public void testAllTenantLiteral() {
        ResourcePatternFilter filter = new ResourcePatternFilter(TOPIC, "Tenant_", PatternType.CONFLUENT_ALL_TENANT_LITERAL);
        assertTrue(filter.matches(new ResourcePattern(TOPIC, "Tenant_Name", LITERAL)));
        assertTrue(filter.matches(new ResourcePattern(TOPIC, "Tenant_", PREFIXED))); // Wildcard literal for tenants

        assertFalse(filter.matches(new ResourcePattern(TOPIC, "Tenant_Name", PREFIXED)));
        assertFalse(filter.matches(new ResourcePattern(TOPIC, "OtherTenant_Name", LITERAL)));
        assertFalse(filter.matches(new ResourcePattern(TOPIC, "OtherTenant_Name", PREFIXED)));
        assertFalse(filter.matches(new ResourcePattern(TOPIC, "OtherTenant_", PREFIXED)));
        assertFalse(filter.matches(new ResourcePattern(TOPIC, "Name", LITERAL)));
        assertFalse(filter.matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));
        assertFalse(filter.matches(new ResourcePattern(TOPIC, "*", LITERAL)));
        assertFalse(filter.matches(new ResourcePattern(GROUP, "Tenant_Name", LITERAL)));
    }

    @Test
    public void testAllTenantPrefixed() {
        ResourcePatternFilter filter = new ResourcePatternFilter(TOPIC, "Tenant_", PatternType.CONFLUENT_ALL_TENANT_PREFIXED);
        assertTrue(filter.matches(new ResourcePattern(TOPIC, "Tenant_Name", PREFIXED)));

        assertFalse(filter.matches(new ResourcePattern(TOPIC, "Tenant_", PREFIXED))); // Wildcard literal for tenants
        assertFalse(filter.matches(new ResourcePattern(TOPIC, "Tenant_Name", LITERAL)));
        assertFalse(filter.matches(new ResourcePattern(TOPIC, "OtherTenant_Name", LITERAL)));
        assertFalse(filter.matches(new ResourcePattern(TOPIC, "OtherTenant_Name", PREFIXED)));
        assertFalse(filter.matches(new ResourcePattern(TOPIC, "OtherTenant_", PREFIXED)));
        assertFalse(filter.matches(new ResourcePattern(TOPIC, "Name", LITERAL)));
        assertFalse(filter.matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));
        assertFalse(filter.matches(new ResourcePattern(TOPIC, "*", LITERAL)));
        assertFalse(filter.matches(new ResourcePattern(GROUP, "Tenant_Name", PREFIXED)));
    }

    @Test
    public void testAllTenantAny() {
        ResourcePatternFilter filter = new ResourcePatternFilter(TOPIC, "Tenant_", PatternType.CONFLUENT_ALL_TENANT_ANY);
        assertTrue(filter.matches(new ResourcePattern(TOPIC, "Tenant_Name", LITERAL)));
        assertTrue(filter.matches(new ResourcePattern(TOPIC, "Tenant_", PREFIXED)));
        assertTrue(filter.matches(new ResourcePattern(TOPIC, "Tenant_Name", PREFIXED)));

        assertFalse(filter.matches(new ResourcePattern(TOPIC, "OtherTenant_Name", LITERAL)));
        assertFalse(filter.matches(new ResourcePattern(TOPIC, "OtherTenant_Name", PREFIXED)));
        assertFalse(filter.matches(new ResourcePattern(TOPIC, "OtherTenant_", PREFIXED)));
        assertFalse(filter.matches(new ResourcePattern(TOPIC, "Name", LITERAL)));
        assertFalse(filter.matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));
        assertFalse(filter.matches(new ResourcePattern(TOPIC, "*", LITERAL)));
        assertFalse(filter.matches(new ResourcePattern(GROUP, "Tenant_Name", LITERAL)));
    }

    @Test
    public void testOnlyTenantMatch() {
        ResourcePatternFilter filter = new ResourcePatternFilter(TOPIC, "Tenant_Name", PatternType.CONFLUENT_ONLY_TENANT_MATCH);
        assertTrue(filter.matches(new ResourcePattern(TOPIC, "Tenant_Name", LITERAL)));
        assertTrue(filter.matches(new ResourcePattern(TOPIC, "Tenant_", PREFIXED)));
        assertTrue(filter.matches(new ResourcePattern(TOPIC, "Tenant_Name", PREFIXED)));
        assertTrue(filter.matches(new ResourcePattern(TOPIC, "Tenant_N", PREFIXED)));

        assertFalse(filter.matches(new ResourcePattern(TOPIC, "Tenant_Name_Something", PREFIXED)));
        assertFalse(filter.matches(new ResourcePattern(TOPIC, "Tenant_Name_Something", LITERAL)));
        assertFalse(filter.matches(new ResourcePattern(TOPIC, "OtherTenant_Name", LITERAL)));
        assertFalse(filter.matches(new ResourcePattern(TOPIC, "OtherTenant_Name", PREFIXED)));
        assertFalse(filter.matches(new ResourcePattern(TOPIC, "OtherTenant_", PREFIXED)));
        assertFalse(filter.matches(new ResourcePattern(TOPIC, "Name", LITERAL)));
        assertFalse(filter.matches(new ResourcePattern(TOPIC, "Name", PREFIXED)));
        assertFalse(filter.matches(new ResourcePattern(TOPIC, "*", LITERAL)));
        assertFalse(filter.matches(new ResourcePattern(GROUP, "Tenant_Name", LITERAL)));
    }
}