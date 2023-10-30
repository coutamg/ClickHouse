#include <Processors/Executors/ExecutingGraph.h>
#include <stack>
#include <Common/Stopwatch.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ExecutingGraph::ExecutingGraph(std::shared_ptr<Processors> processors_, bool profile_processors_)
    : processors(std::move(processors_))
    , profile_processors(profile_processors_)
{
    uint64_t num_processors = processors->size();
    nodes.reserve(num_processors);
    source_processors.reserve(num_processors);

    /// Create nodes.
    for (uint64_t node = 0; node < num_processors; ++node)
    {
        IProcessor * proc = processors->at(node).get();
        processors_map[proc] = node;
        nodes.emplace_back(std::make_unique<Node>(proc, node));

        // source 的 input 是空的，因为 source 是需要从表里面 scan 数据
        bool is_source = proc->getInputs().empty();
        source_processors.emplace_back(is_source);
    }

    /// Create edges.
    for (uint64_t node = 0; node < num_processors; ++node)
        addEdges(node);
}

ExecutingGraph::Edge & ExecutingGraph::addEdge(Edges & edges, Edge edge, const IProcessor * from, const IProcessor * to)
{
    auto it = processors_map.find(to);
    if (it == processors_map.end())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Processor {} was found as {} for processor {}, but not found in list of processors",
            to->getName(),
            edge.backward ? "input" : "output",
            from->getName());

    edge.to = it->second;
    auto & added_edge = edges.emplace_back(std::move(edge));
    added_edge.update_info.id = &added_edge;
    return added_edge;
}

// 将当前新的 node 加入到 有向图里面
bool ExecutingGraph::addEdges(uint64_t node)
{
    IProcessor * from = nodes[node]->processor;

    bool was_edge_added = false;

    /// Add backward edges from input ports. 获取 input list
    auto & inputs = from->getInputs();
    auto from_input = nodes[node]->back_edges.size();

    if (from_input < inputs.size())
    {
        was_edge_added = true;

        // 获取 from_input + 1 的 IProcessor
        for (auto it = std::next(inputs.begin(), from_input); it != inputs.end(); ++it, ++from_input)
        {
            // 通过 InputPort 的 OutputPort(哪个 process 发送数据过来)，找到谁发的数据
            const IProcessor * to = &it->getOutputPort().getProcessor();
            auto output_port_number = to->getOutputPortNumber(&it->getOutputPort());
            Edge edge(0, true, from_input, output_port_number, &nodes[node]->post_updated_input_ports);
            auto & added_edge = addEdge(nodes[node]->back_edges, std::move(edge), from, to);
            it->setUpdateInfo(&added_edge.update_info);
        }
    }

    /// Add direct edges from output ports.
    auto & outputs = from->getOutputs();
    auto from_output = nodes[node]->direct_edges.size();

    if (from_output < outputs.size())
    {
        was_edge_added = true;

        for (auto it = std::next(outputs.begin(), from_output); it != outputs.end(); ++it, ++from_output)
        {
           // 通过 OutputPort 的 InputPort(哪个 process 接收数据过来)，找到谁接收数据
            const IProcessor * to = &it->getInputPort().getProcessor();
            auto input_port_number = to->getInputPortNumber(&it->getInputPort());
            Edge edge(0, false, input_port_number, from_output, &nodes[node]->post_updated_output_ports);
            auto & added_edge = addEdge(nodes[node]->direct_edges, std::move(edge), from, to);
            it->setUpdateInfo(&added_edge.update_info);
        }
    }

    return was_edge_added;
}

bool ExecutingGraph::expandPipeline(std::stack<uint64_t> & stack, uint64_t pid)
{
    auto & cur_node = *nodes[pid];
    Processors new_processors;

    try
    {
        new_processors = cur_node.processor->expandPipeline();
    }
    catch (...)
    {
        cur_node.exception = std::current_exception();
        return false;
    }

    {
        std::lock_guard guard(processors_mutex);
        /// Do not add new processors to existing list, since the query was already cancelled.
        if (cancelled)
        {
            for (auto & processor : new_processors)
                processor->cancel();
            return false;
        }
        processors->insert(processors->end(), new_processors.begin(), new_processors.end());

        // Do not consider sources added during pipeline expansion as cancelable to avoid tricky corner cases (e.g. ConvertingAggregatedToChunksWithMergingSource cancellation)
        source_processors.resize(source_processors.size() + new_processors.size(), false);
    }

    uint64_t num_processors = processors->size();
    std::vector<uint64_t> back_edges_sizes(num_processors, 0);
    std::vector<uint64_t> direct_edge_sizes(num_processors, 0);

    for (uint64_t node = 0; node < nodes.size(); ++node)
    {
        direct_edge_sizes[node] = nodes[node]->direct_edges.size();
        back_edges_sizes[node] = nodes[node]->back_edges.size();
    }

    nodes.reserve(num_processors);

    while (nodes.size() < num_processors)
    {
        auto * processor = processors->at(nodes.size()).get();
        if (processors_map.contains(processor))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Processor {} was already added to pipeline", processor->getName());

        processors_map[processor] = nodes.size();
        nodes.emplace_back(std::make_unique<Node>(processor, nodes.size()));
    }

    std::vector<uint64_t> updated_nodes;

    for (uint64_t node = 0; node < num_processors; ++node)
    {
        if (addEdges(node))
            updated_nodes.push_back(node);
    }

    for (auto updated_node : updated_nodes)
    {
        auto & node = *nodes[updated_node];

        size_t num_direct_edges = node.direct_edges.size();
        size_t num_back_edges = node.back_edges.size();

        std::lock_guard guard(node.status_mutex);

        for (uint64_t edge = back_edges_sizes[updated_node]; edge < num_back_edges; ++edge)
            node.updated_input_ports.emplace_back(edge);

        for (uint64_t edge = direct_edge_sizes[updated_node]; edge < num_direct_edges; ++edge)
            node.updated_output_ports.emplace_back(edge);

        if (node.status == ExecutingGraph::ExecStatus::Idle)
        {
            node.status = ExecutingGraph::ExecStatus::Preparing;
            stack.push(updated_node);
        }
    }

    return true;
}

void ExecutingGraph::initializeExecution(Queue & queue)
{
    std::stack<uint64_t> stack;

    /// Add childless processors to stack.
    uint64_t num_processors = nodes.size();
    for (uint64_t proc = 0; proc < num_processors; ++proc)
    {
        // 将不存在 direct_edges 的 Node 放入栈中,
        // 对于 plan tree 来说，最顶层的 node 是向用户发送数据的，所以没有 output 了
        if (nodes[proc]->direct_edges.empty())
        {
            stack.push(proc);
            /// do not lock mutex, as this function is executed in single thread
            nodes[proc]->status = ExecutingGraph::ExecStatus::Preparing;
        }
    }

    Queue async_queue;

    // 从 plan tree 的顶向下开始 update node
    while (!stack.empty())
    {
        uint64_t proc = stack.top();
        stack.pop();
        // 更新 Node 状态.
        updateNode(proc, queue, async_queue);

        if (!async_queue.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Async is only possible after work() call. Processor {}",
                            async_queue.front()->processor->getName());
    }
}

// 根据每个 Node 的 updated_input_ports，updated_output_ports 状态来逐渐的更新相邻
// Node 中 processor 的状态，使得 pipeline 不断运行下去
bool ExecutingGraph::updateNode(uint64_t pid, Queue & queue, Queue & async_queue)
{
    std::stack<Edge *> updated_edges;
    std::stack<uint64_t> updated_processors;
    updated_processors.push(pid);

    std::shared_lock read_lock(nodes_mutex);

    while (!updated_processors.empty() || !updated_edges.empty())
    {
        std::optional<std::unique_lock<std::mutex>> stack_top_lock;

        // 处理 updated_edges 中 edge 指向的 node.
        if (updated_processors.empty())
        {
            auto * edge = updated_edges.top();
            updated_edges.pop();

            /// Here we have ownership on edge, but node can be concurrently accessed.

            auto & node = *nodes[edge->to];

            std::unique_lock lock(node.status_mutex);

            ExecutingGraph::ExecStatus status = node.status;

            if (status != ExecutingGraph::ExecStatus::Finished)
            {
                if (edge->backward)
                    node.updated_output_ports.push_back(edge->output_port_number);
                else
                    node.updated_input_ports.push_back(edge->input_port_number);

                if (status == ExecutingGraph::ExecStatus::Idle)
                {
                    node.status = ExecutingGraph::ExecStatus::Preparing;
                    updated_processors.push(edge->to);
                    stack_top_lock = std::move(lock);
                }
                else
                    nodes[edge->to]->processor->onUpdatePorts();
            }
        }

        // 处理 updated_processors 中 node,更新 node 状态.
        if (!updated_processors.empty())
        {
            pid = updated_processors.top();
            updated_processors.pop();
            /// In this method we have ownership on node.
            auto & node = *nodes[pid];

            bool need_expand_pipeline = false;

            if (!stack_top_lock)
                stack_top_lock.emplace(node.status_mutex);

            {
#ifndef NDEBUG
                Stopwatch watch;
#endif

                std::unique_lock<std::mutex> lock(std::move(*stack_top_lock));

                try
                {
                    auto & processor = *node.processor;
                    IProcessor::Status last_status = node.last_processor_status;
                    // 对于第一次初始化来说，select 会从顶层 porcessor 开始，此时会返回
                    // Status::NeedData(参考 IOutputFormat.cpp::prepare)
                    // 一般来说最后会 plan tree 最终会调用到 ISource::prepare
                    IProcessor::Status status = processor.prepare(node.updated_input_ports, node.updated_output_ports);
                    node.last_processor_status = status;

                    if (profile_processors)
                    {
                        /// NeedData
                        if (last_status != IProcessor::Status::NeedData && status == IProcessor::Status::NeedData)
                        {
                            processor.input_wait_watch.restart();
                        }
                        else if (last_status == IProcessor::Status::NeedData && status != IProcessor::Status::NeedData)
                        {
                            processor.input_wait_elapsed_us += processor.input_wait_watch.elapsedMicroseconds();
                        }

                        /// PortFull
                        if (last_status != IProcessor::Status::PortFull && status == IProcessor::Status::PortFull)
                        {
                            processor.output_wait_watch.restart();
                        }
                        else if (last_status == IProcessor::Status::PortFull && status != IProcessor::Status::PortFull)
                        {
                            processor.output_wait_elapsed_us += processor.output_wait_watch.elapsedMicroseconds();
                        }
                    }
                }
                catch (...)
                {
                    node.exception = std::current_exception();
                    return false;
                }

#ifndef NDEBUG
                node.preparation_time_ns += watch.elapsed();
#endif

                node.updated_input_ports.clear();
                node.updated_output_ports.clear();

                // 通过 Inport 来确定当前 node 的 status
                switch (node.last_processor_status)
                {
                    // NeedData 将 Node.status 从 Preparing -> Idle.
                    case IProcessor::Status::NeedData:
                    case IProcessor::Status::PortFull:
                    {
                        node.status = ExecutingGraph::ExecStatus::Idle;
                        break;
                    }
                    case IProcessor::Status::Finished:
                    {
                        node.status = ExecutingGraph::ExecStatus::Finished;
                        break;
                    }
                    case IProcessor::Status::Ready:
                    {
                        // 对于 ExecutingGraph::initializeExecution 来说，最后
                        // 会调用到 ISource::prepare, 此时由于没有 InputPort，所以
                        // 上面的 prepare 直接返回 Status::Ready, 走到这里，因此
                        // updateNode 返回
                        node.status = ExecutingGraph::ExecStatus::Executing;
                        queue.push(&node);
                        break;
                    }
                    case IProcessor::Status::Async:
                    {
                       // 第一次初始化时，node 的状态为 Executing
                        node.status = ExecutingGraph::ExecStatus::Executing;
                        async_queue.push(&node);
                        break;
                    }
                    case IProcessor::Status::ExpandPipeline:
                    {
                        need_expand_pipeline = true;
                        break;
                    }
                }

                // 将 Node 相邻的待更新 Edge 放入 update_edges 这个栈中.
                if (!need_expand_pipeline)
                {
                    /// If you wonder why edges are pushed in reverse order,
                    /// it is because updated_edges is a stack, and we prefer to get from stack
                    /// input ports firstly, and then outputs, both in-order.
                    ///
                    /// Actually, there should be no difference in which order we process edges.
                    /// However, some tests are sensitive to it (e.g. something like SELECT 1 UNION ALL 2).
                    /// Let's not break this behaviour so far.

                    for (auto it = node.post_updated_output_ports.rbegin(); it != node.post_updated_output_ports.rend(); ++it)
                    {
                        auto * edge = static_cast<ExecutingGraph::Edge *>(*it);
                        updated_edges.push(edge);
                        edge->update_info.trigger();
                    }

                    for (auto it = node.post_updated_input_ports.rbegin(); it != node.post_updated_input_ports.rend(); ++it)
                    {
                        auto * edge = static_cast<ExecutingGraph::Edge *>(*it);
                        updated_edges.push(edge);
                        edge->update_info.trigger();
                    }

                    node.post_updated_input_ports.clear();
                    node.post_updated_output_ports.clear();
                }
            }

            if (need_expand_pipeline)
            {
                // We do not need to upgrade lock atomically, so we can safely release shared_lock and acquire unique_lock
                read_lock.unlock();
                {
                    std::unique_lock lock(nodes_mutex);
                    if (!expandPipeline(updated_processors, pid))
                        return false;
                }
                read_lock.lock();

                /// Add itself back to be prepared again.
                updated_processors.push(pid);
            }
        }
    }

    return true;
}

void ExecutingGraph::cancel(bool cancel_all_processors)
{
    std::exception_ptr exception_ptr;

    {
        std::lock_guard guard(processors_mutex);
        uint64_t num_processors = processors->size();
        for (uint64_t proc = 0; proc < num_processors; ++proc)
        {
            try
            {
                /// Stop all processors in the general case, but in a specific case
                /// where the pipeline needs to return a result on a partially read table,
                /// stop only the processors that read from the source
                if (cancel_all_processors || source_processors.at(proc))
                {
                    IProcessor * processor = processors->at(proc).get();
                    processor->cancel();
                }
            }
            catch (...)
            {
                if (!exception_ptr)
                    exception_ptr = std::current_exception();

                /// Log any exception since:
                /// a) they are pretty rare (the only that I know is from
                ///    RemoteQueryExecutor)
                /// b) there can be exception during query execution, and in this
                ///    case, this exception can be ignored (not showed to the user).
                tryLogCurrentException("ExecutingGraph");
            }
        }
        if (cancel_all_processors)
            cancelled = true;
    }

    if (exception_ptr)
        std::rethrow_exception(exception_ptr);
}

}
