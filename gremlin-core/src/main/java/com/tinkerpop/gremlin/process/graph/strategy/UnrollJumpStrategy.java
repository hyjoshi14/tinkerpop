package com.tinkerpop.gremlin.process.graph.strategy;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategy;
import com.tinkerpop.gremlin.process.graph.step.map.JumpStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class UnrollJumpStrategy implements TraversalStrategy.FinalTraversalStrategy {

    public void apply(final Traversal traversal) {
        TraversalHelper.getStepsOfClass(JumpStep.class, traversal).stream()
                .filter(JumpStep::unRollable)
                .forEach(toStep -> {
                    final Step fromStep = TraversalHelper.getAs(toStep.jumpAs, traversal);
                    final List<Step> stepsToClone = TraversalHelper.isolateSteps(fromStep, toStep);
                    for (int i = 0; i < toStep.loops - 1; i++) {
                        for (int j = stepsToClone.size() - 1; j >= 0; j--) {
                            final Step clonedStep = TraversalHelper.cloneStep(stepsToClone.get(j));
                            TraversalHelper.insertStep(clonedStep, traversal.getSteps().indexOf(fromStep) + 1, traversal);
                        }
                    }
                    TraversalHelper.removeStep(toStep, traversal);
                });
    }
}
