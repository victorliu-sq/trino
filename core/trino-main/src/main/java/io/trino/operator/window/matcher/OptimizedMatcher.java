package io.trino.operator.window.matcher;

import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.window.matcher.Instruction;
import io.trino.operator.window.matcher.IntList;
import io.trino.operator.window.matcher.MatchResult;
import io.trino.operator.window.matcher.Matcher;
import io.trino.operator.window.matcher.Program;
import io.trino.operator.window.matcher.ThreadEquivalence;
import io.trino.operator.window.pattern.LabelEvaluator;
import io.trino.operator.window.pattern.MatchAggregation.MatchAggregationInstantiator;
import io.trino.operator.window.pattern.PhysicalValueAccessor;
import io.trino.sql.planner.LocalExecutionPlanner.MatchAggregationLabelDependency;

import java.util.List;

import static io.trino.operator.window.matcher.MatchResult.NO_MATCH;

public class OptimizedMatcher extends Matcher
{
    public OptimizedMatcher(Program program, List<List<PhysicalValueAccessor>> accessors,
                           List<MatchAggregationLabelDependency> labelDependencies,
                           List<MatchAggregationInstantiator> aggregations)
    {
        super(program, accessors, labelDependencies, aggregations);
    }

    @Override
    public MatchResult run(LabelEvaluator labelEvaluator, LocalMemoryContext memoryContext,
                          AggregatedMemoryContext aggregationsMemoryContext)
    {
//        System.out.println("OptimizedMatcher.run: Starting execution on node " + System.currentTimeMillis());

        // Pre-optimization (e.g., logging, setup)
        long startTime = System.nanoTime();

        // Call parent’s run method
        MatchResult result = super.run(labelEvaluator, memoryContext, aggregationsMemoryContext);

        // Post-optimization (e.g., timing)
        long endTime = System.nanoTime();
//        System.out.println("OptimizedMatcher.run: Execution took " + (endTime - startTime) / 1_000_000 + " ms");

        return result;
    }
}
