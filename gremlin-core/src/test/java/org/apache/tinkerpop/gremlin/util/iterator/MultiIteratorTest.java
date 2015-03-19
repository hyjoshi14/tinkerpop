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
package org.apache.tinkerpop.gremlin.util.iterator;

import org.apache.tinkerpop.gremlin.process.traversal.FastNoSuchElementException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class MultiIteratorTest {
    @Test
    public void shouldNotHaveNextIfNoIteratorsAreAdded() {
        final Iterator<String> itty = new MultiIterator<>();
        assertFalse(itty.hasNext());
    }

    @Test(expected = FastNoSuchElementException.class)
    public void shouldThrowFastNoSuchElementExceptionIfNoIteratorsAreAdded() {
        final Iterator<String> itty = new MultiIterator<>();
        itty.next();
    }

    @Test
    public void shouldNotHaveNextIfEmptyIteratorIsAdded() {
        final MultiIterator<String> itty = new MultiIterator<>();
        itty.addIterator(EmptyIterator.instance());
        assertFalse(itty.hasNext());
    }

    @Test(expected = FastNoSuchElementException.class)
    public void shouldThrowFastNoSuchElementExceptionIfEmptyIteratorIsAdded() {
        final MultiIterator<String> itty = new MultiIterator<>();
        itty.addIterator(EmptyIterator.instance());
        itty.next();
    }

    @Test
    public void shouldNotHaveNextIfEmptyIteratorsAreAdded() {
        final MultiIterator<String> itty = new MultiIterator<>();
        itty.addIterator(EmptyIterator.instance());
        itty.addIterator(EmptyIterator.instance());
        itty.addIterator(EmptyIterator.instance());
        itty.addIterator(EmptyIterator.instance());
        assertFalse(itty.hasNext());
    }

    @Test(expected = FastNoSuchElementException.class)
    public void shouldThrowFastNoSuchElementExceptionIfEmptyIteratorsAreAdded() {
        final MultiIterator<String> itty = new MultiIterator<>();
        itty.addIterator(EmptyIterator.instance());
        itty.addIterator(EmptyIterator.instance());
        itty.addIterator(EmptyIterator.instance());
        itty.addIterator(EmptyIterator.instance());
        itty.next();
    }

    @Test
    public void shouldIterateWhenMultipleIteratorsAreAdded() {
        final List<String> list = new ArrayList<>();
        list.add("test1");
        list.add("test2");
        list.add("test3");

        final MultiIterator<String> itty = new MultiIterator<>();
        itty.addIterator(EmptyIterator.instance());
        itty.addIterator(list.iterator());

        assertEquals("test1", itty.next());
        assertEquals("test2", itty.next());
        assertEquals("test3", itty.next());
        assertFalse(itty.hasNext());
    }

    @Test
    public void shouldClearIterators() {
        final List<String> list = new ArrayList<>();
        list.add("test1");
        list.add("test2");
        list.add("test3");

        final MultiIterator<String> itty = new MultiIterator<>();
        itty.addIterator(list.iterator());

        itty.clear();

        assertFalse(itty.hasNext());
    }
}
