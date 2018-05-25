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
import org.apache.tinkerpop.gremlin.process.traversal.step.StepConfiguration;

import java.io.Serializable;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StepConfigurationProxy<T extends StepConfiguration> implements Serializable {

    private final Configuration configuration;
    private final Class<T> stepConfigurationClass;

    public StepConfigurationProxy(final T stepConfiguration) {
        this((Class<T>) stepConfiguration.getClass(), stepConfiguration.getConfiguration());
    }

    public StepConfigurationProxy(final Class<T> stepConfigurationClass, final Configuration configuration) {
        this.configuration = configuration;
        this.stepConfigurationClass = stepConfigurationClass;
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }

    public Class<T> getStepConfigurationClass() {
        return this.stepConfigurationClass;
    }
}