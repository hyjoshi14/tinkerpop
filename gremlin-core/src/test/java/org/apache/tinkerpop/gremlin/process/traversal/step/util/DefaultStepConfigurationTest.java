/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.step.util;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PageRankVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnectionException;
import org.apache.tinkerpop.gremlin.process.remote.traversal.RemoteTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepConfiguration;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.NoSuchElementException;

import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DefaultStepConfigurationTest {

    @Test
    public void shouldApplyDefaultConfiguration() throws Exception {
        final Traversal t = __.V().pageRank().with(new DefaultStepConfiguration("modulateBy", "xxx"));
        final Step s = t.asAdmin().getEndStep();
        assertEquals("xxx", FieldUtils.readField(s, "pageRankProperty", true));
    }

    @Test
    public void shouldApplyDefaultConfigurationWithClassValidation() throws Exception {
        final Traversal t = __.V().pageRank().with(new DefaultStepConfiguration(PageRankVertexProgramStep.class, "modulateBy", "xxx"));
        final Step s = t.asAdmin().getEndStep();
        assertEquals("xxx", FieldUtils.readField(s, "pageRankProperty", true));
    }

    @Test
    public void shouldApplyDefaultConfigurationInOrder() throws Exception {
        final LinkedHashMap<String, List<Object>> methods = new LinkedHashMap<>();
        methods.put("setY", Collections.singletonList(100L));
        methods.put("setX", Collections.singletonList("xxx"));
        methods.put("setZ", Collections.singletonList("zzz" ));
        final StepConfiguration<Step> conf = new DefaultStepConfiguration(methods);
        final MockStep step = new MockStep(__.__().asAdmin());

        conf.accept(step);

        assertThat(step.list, contains(100L, "Xxxx", "Zzzz"));
    }

    @Test
    public void shouldGenerateConfiguration() throws Exception {
        final LinkedHashMap<String, List<Object>> methods = new LinkedHashMap<>();
        methods.put("setY", Collections.singletonList(100L));
        methods.put("setX", Collections.singletonList("xxx"));
        methods.put("setZ", Collections.singletonList("zzz" ));
        final StepConfiguration<Step> conf = new DefaultStepConfiguration(methods);
        final MapConfiguration c = (MapConfiguration) conf.getConfiguration();
        c.setDelimiterParsingDisabled(false);

        assertEquals(100L, c.getList("setY").get(0));
        assertEquals("xxx", c.getList("setX").get(0));
        assertEquals("zzz", c.getList("setZ").get(0));
    }

    @Test(expected = IllegalStateException.class)
    public void shouldValidateClass() {
        __.V().pageRank().with(new DefaultStepConfiguration(MockStep.class, "modulateBy", "xxx"));
    }

    @Test
    public void shouldAllowNoSuchMethodIfUsingRemote() {
        // create a fake remote
        final GraphTraversalSource g = EmptyGraph.instance().traversal().withRemote(new RemoteConnection() {
            @Override
            public <E> Iterator<Traverser.Admin<E>> submit(final Traversal<?, E> traversal) throws RemoteConnectionException {
                return null;
            }

            @Override
            public <E> RemoteTraversal<?, E> submit(final Bytecode bytecode) throws RemoteConnectionException {
                return null;
            }

            @Override
            public void close() throws Exception {

            }
        });

        // try to set a fake configuration option - lack of exception is good. not really sure how else to directly
        // assert this
        final LinkedHashMap<String, List<Object>> methods = new LinkedHashMap<>();
        methods.put("setFakeyFakerton", Collections.singletonList(100L));
        final StepConfiguration<Step> conf = new DefaultStepConfiguration(methods);
        g.V().with(conf);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldNotAllowNoSuchMethodUnlessUsingRemote() {
        final GraphTraversalSource g = EmptyGraph.instance().traversal();

        // try to set a fake configuration option
        final LinkedHashMap<String, List<Object>> methods = new LinkedHashMap<>();
        methods.put("setFakeyFakerton", Collections.singletonList(100L));
        final StepConfiguration<Step> conf = new DefaultStepConfiguration(methods);
        g.V().with(conf);
    }

    static class MockStep extends AbstractStep {

        List<Object> list = new ArrayList<>();

        MockStep(final Traversal.Admin t) {
            super(t);
        }

        public void setX(final String s) {
            list.add("X" + s);
        }

        public void setY(final Long s) {
            list.add(s);
        }

        public void setZ(final String s) {
            list.add("Z" + s);
        }


        @Override
        protected Traverser.Admin processNextStart() throws NoSuchElementException {
            return null;
        }
    }
}
