#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

ITransformingStep::ITransformingStep(DataStream input_stream, Block output_header, Traits traits, bool collect_processors_)
    : transform_traits(std::move(traits.transform_traits))
    , collect_processors(collect_processors_)
    , data_stream_traits(std::move(traits.data_stream_traits))
{
    input_streams.emplace_back(std::move(input_stream));
    output_stream = createOutputStream(input_streams.front(), std::move(output_header), data_stream_traits);
}

DataStream ITransformingStep::createOutputStream(
    const DataStream & input_stream,
    Block output_header,
    const DataStreamTraits & stream_traits)
{
    DataStream output_stream{.header = std::move(output_header)};

    output_stream.has_single_port = stream_traits.returns_single_stream
                                     || (input_stream.has_single_port && stream_traits.preserves_number_of_streams);

    if (stream_traits.preserves_sorting)
    {
        output_stream.sort_description = input_stream.sort_description;
        output_stream.sort_scope = input_stream.sort_scope;
    }

    return output_stream;
}

// 对拉取的数据进行处理的算子，这里的 pipelines 是下层 node(child node) 的输出
QueryPipelineBuilderPtr ITransformingStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings)
{
    // 创建算子对应的 Transformer，添加 Transformer 到 Pipeline. 例如创建 agg 算子对数据处理的 Transformer
    if (collect_processors)
    {
        QueryPipelineProcessorsCollector collector(*pipelines.front(), this);
        // 1.虚方法，将当前算子对应的 transformer 添加到 pipeline.
        transformPipeline(*pipelines.front(), settings);
        // 2.收集 processor.
        processors = collector.detachProcessors();
    }
    else
        transformPipeline(*pipelines.front(), settings);

    return std::move(pipelines.front());
}

void ITransformingStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

}
