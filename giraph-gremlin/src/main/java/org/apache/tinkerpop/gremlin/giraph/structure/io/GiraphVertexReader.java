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
package org.apache.tinkerpop.gremlin.giraph.structure.io;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.tinkerpop.gremlin.giraph.process.computer.GiraphVertex;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;

import java.io.IOException;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GiraphVertexReader extends VertexReader {

    private RecordReader<NullWritable, VertexWritable> recordReader;
    private final GraphFilter graphFilter;
    private final boolean graphFilterAware;

    public GiraphVertexReader(final RecordReader<NullWritable, VertexWritable> recordReader, final boolean graphFilterAware, final GraphFilter graphFilter) {
        this.recordReader = recordReader;
        this.graphFilterAware = graphFilterAware;
        this.graphFilter = graphFilter.clone();
    }

    @Override
    public void initialize(final InputSplit inputSplit, final TaskAttemptContext context) throws IOException, InterruptedException {
        this.recordReader.initialize(inputSplit, context);
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
        if (this.graphFilterAware) {
            return this.recordReader.nextKeyValue();
        } else {
            while (true) {
                if (this.recordReader.nextKeyValue()) {
                    final VertexWritable vertexWritable = this.recordReader.getCurrentValue();
                    final Optional<StarGraph.StarVertex> vertex = this.graphFilter.applyGraphFilter(vertexWritable.get());
                    if (vertex.isPresent()) {
                        vertexWritable.set(vertex.get());
                        return true;
                    }
                } else {
                    return false;
                }
            }
        }
    }

    @Override
    public Vertex getCurrentVertex() throws IOException, InterruptedException {
        return new GiraphVertex(this.recordReader.getCurrentValue());
    }

    @Override
    public void close() throws IOException {
        this.recordReader.close();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return this.recordReader.getProgress();
    }
}
