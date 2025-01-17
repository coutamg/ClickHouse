#include <Processors/QueryPlan/ISourceStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

ISourceStep::ISourceStep(DataStream output_stream_)
{
    output_stream = std::move(output_stream_);
}

// 从存储层拉取数据的算子
QueryPipelineBuilderPtr ISourceStep::updatePipeline(QueryPipelineBuilders, const BuildQueryPipelineSettings & settings)
{
    // 1.实例化一个 QueryPipelineBuilder，用于构键后续的 Pipeline.
    auto pipeline = std::make_unique<QueryPipelineBuilder>();

    /// For `Source` step, since it's not add new Processors to `pipeline->pipe`
    /// in `initializePipeline`, but make an assign with new created Pipe.
    /// And Processors for the Step is added here. So we do not need to use
    /// `QueryPipelineProcessorsCollector` to collect Processors.
    // 初始化 Pipeline
    // 2.虚方法，调用对应算子的初始化 pipeline
    initializePipeline(*pipeline, settings);

    /// But we need to set QueryPlanStep manually for the Processors, which
    /// will be used in `EXPLAIN PIPELINE`
    for (auto & processor : processors)
    {
        processor->setQueryPlanStep(this);
    }
    return pipeline;
}

void ISourceStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
