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
import org.apache.commons.lang.reflect.MethodUtils;
import org.apache.tinkerpop.gremlin.process.remote.traversal.strategy.decoration.RemoteStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepConfiguration;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A basic {@link StepConfiguration} implementation that uses reflection to set methods on the step to which the
 * configuration will be applied. While use of reflection isn't quite as nice as direct application of configuration
 * options to a step, this implementation is serialization ready and thus requires no additional work from the
 * developer to get a step option ready for usage. If using this implementation, it is of extreme importance that
 * the developer implement solid test coverage to ensure that reflection calls will work at runtime as compilation
 * errors will not be raised under this approach.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DefaultStepConfiguration implements StepConfiguration<Step> {

    private final Map<String, List<Object>> conf;
    private final Class<? extends Step> expects;

    /**
     * Creates a new {@code DefaultStepConfiguration}.
     *
     * @param method to call on the step
     * @param args the arguments to pass to the method
     */
    public DefaultStepConfiguration(final String method, final Object... args) {
        this(null, method, args);
    }

    /**
     * Creates a new {@code DefaultStepConfiguration}.
     *
     * @param methods a map of methods to call when configuring a step where the keys are the method names and the
     *                values are the list of arguments to apply to that method
     */
    public DefaultStepConfiguration(final LinkedHashMap<String, List<Object>> methods) {
        this(null, methods);
    }

    /**
     * Creates a new {@code DefaultStepConfiguration} with a validation option to ensure that the configuration is
     * applied to the right type of step.
     *
     * @param expects the step type that this configuration should be applied to
     * @param method to call on the step
     * @param args the arguments to pass to the method
     */
    public DefaultStepConfiguration(final Class<? extends Step> expects, final String method, final Object... args) {
        if (null == method || method.isEmpty()) throw new IllegalArgumentException("method may not be null or empty");
        conf = new LinkedHashMap<>();
        conf.put(method, Arrays.asList(args));
        this.expects = expects;
    }

    /**
     * Creates a new {@code DefaultStepConfiguration} with a validation option to ensure that the configuration is
     * applied to the right type of step.
     *
     * @param expects the step type that this configuration should be applied to
     * @param methods a map of methods to call when configuring a step where the keys are the method names and the
     *                values are the list of arguments to apply to that method
     */
    public DefaultStepConfiguration(final Class<? extends Step> expects, final LinkedHashMap<String, List<Object>> methods) {
        if (null == methods || methods.isEmpty()) throw new IllegalArgumentException("methods may not be null or empty");
        if (IteratorUtils.anyMatch(methods.keySet().iterator(), k -> null == k || k.isEmpty())) throw new IllegalArgumentException("no key of methods map may be null or empty");
        conf = methods;
        this.expects = expects;
    }

    private DefaultStepConfiguration() {
        // for gyro's sake.........
        conf = Collections.emptyMap();
        expects = null;
    }

    @Override
    public void accept(final Step step) {
        final Optional<Class<? extends Step>> opt = Optional.ofNullable(expects);
        if (opt.isPresent() && !opt.get().isAssignableFrom(step.getClass())) {
            throw new IllegalStateException(String.format("Could not apply step configuration of %s to %s", conf, step.getClass().getName()));
        }

        for (Map.Entry<String, List<Object>> kv : conf.entrySet()) {
            try {
                MethodUtils.invokeMethod(step, kv.getKey(), kv.getValue().toArray());
            } catch (NoSuchMethodException nsme) {
                if (!step.getTraversal().asAdmin().getStrategies().getStrategy(RemoteStrategy.class).isPresent())
                    throw new IllegalStateException(String.format("Step configuration of %s with args of %s cannot be applied to %s",
                            kv.getKey(), kv.getValue(), step.getClass().getName()), nsme);
            } catch (Exception ex) {
                throw new IllegalStateException(String.format("Step configuration of %s with args of %s cannot be applied to %s",
                        kv.getKey(), kv.getValue(), step.getClass().getName()), ex);
            }
        }
    }

    public static StepConfiguration create(final Configuration conf) {
        final LinkedHashMap<String,List<Object>> m = new LinkedHashMap<>();
        final Iterator<String> keys = conf.getKeys();
        while (keys.hasNext()) {
            final String key = keys.next();
            m.put(key, conf.getList(key));
        }
        return new DefaultStepConfiguration(m);
    }

    @Override
    public Configuration getConfiguration() {
        return new MapConfiguration(conf);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final DefaultStepConfiguration that = (DefaultStepConfiguration) o;

        return conf.equals(that.conf);
    }

    @Override
    public int hashCode() {
        return conf.hashCode();
    }
}
