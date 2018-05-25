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
package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.DefaultStepConfiguration;

import java.util.function.Consumer;

/**
 * A {@code WithOption} can be supplied to {@link GraphTraversal#with(StepConfiguration)} and is designed to modulate a
 * {@link Step} in some way. As {@code WithStep} is a {@code Consumer} that accepts a {@link Step}, the implementation
 * can modify that step in any way it deems necessary. Typical usage for those adding to the Gremlin language in some
 * way would be to provide an expression that returns a {@link DefaultStepConfiguration}.
 * <p/>
 * To work properly with TinkerPop serialization, implementations should provide a static
 * {@code StepConfiguration create(Configuration)} method.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface StepConfiguration<S extends Step> extends Consumer<S> {

    /**
     * Get the configuration representation of this strategy. This is useful for converting a strategy into a
     * serialized form.
     *
     * @return the configuration used to create this strategy
     */
    public default Configuration getConfiguration() {
        return new BaseConfiguration();
    }
}
