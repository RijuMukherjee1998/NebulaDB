#include <memory>
#include <vector>

#include "../headers/Executor.h"
#include "../headers/PageOps.h"

using EC = QueryEngine::ExecutionContext;
QueryEngine::ExecResults QueryEngine::InsertNode::execute(EC& ctx)
{
    ExecResults results;

    //TODO::
    // do the insert now.should pagedata handle it?
    return results;

}

QueryEngine::ExecResults QueryEngine::IndexNode::execute(EC& ctx)
{
    ExecResults results;

    //TODO::
    // do the indexing now.should pagedata handle it?
    return results;

}

QueryEngine::ExecResults QueryEngine::UpdateNode::execute(EC& ctx)
{
    ExecResults results;
    if(predicate != nullptr){
        results = predicate->execute(ctx);
    }
    //TODO::
    // do the update now.should pagedata handle it?
    return results;
}

QueryEngine::ExecResults QueryEngine::DeleteNode::execute(EC& ctx)
{
    ExecResults results;
    if(predicate != nullptr){
        results = predicate->execute(ctx);
    }
    StorageEngine::PageOps pg_ops(ctx.schema,ctx.pg_dir,ctx.idx_table);
    pg_ops.DeleteRows(results);
    return results;
}

QueryEngine::ExecResults QueryEngine::SelectNode::execute(EC& ctx)
{
    ExecResults prev_result;
    if(predicate != nullptr){
        prev_result = predicate->execute(ctx);
    }
    StorageEngine::PageOps pg_ops(ctx.schema,ctx.pg_dir,ctx.idx_table);
    ExecResults results = pg_ops.ProjectOnRows(prev_result, projections); 
    return results;
}

QueryEngine::ExecResults QueryEngine::UnionNode::execute(EC& ctx)
{
    std::unique_ptr<std::vector<ROW_ID>> results = std::make_unique<std::vector<ROW_ID>>();
    for(auto& filterNode : filterNodes)
    {
        auto temp_out  = filterNode->execute(ctx);
        std::visit([&](auto& ptr) {
            using T = std::decay_t<decltype(*ptr)>;
            if constexpr (std::is_same_v<T, std::vector<ROW_ID>>) {
                for (auto row : *ptr) {
                    results->emplace_back(row);
                }
            }
        },temp_out);
    }
    return results;
}

QueryEngine::ExecResults QueryEngine::FilterNode::execute(EC& ctx)
{
    std::unique_ptr<std::vector<ROW_ID>> results = std::make_unique<std::vector<ROW_ID>>();
    for(auto& scanNode : scanNodes)
    {
        ExecResults temp_out;
        if(scanNode->type){
            temp_out = ((IndexScanNode*)scanNode.get())->execute(ctx);
        }
        else{
            temp_out = ((SeqScanNode*)scanNode.get())->execute(ctx);
        }
        std::visit([&](auto& ptr) {
            using T = std::decay_t<decltype(*ptr)>;
            if constexpr (std::is_same_v<T, std::vector<ROW_ID>>) {
                for (auto row : *ptr) {
                    results->emplace_back(row);
                }
            }
        },temp_out);
    }
    return results;
}

QueryEngine::ExecResults QueryEngine::IndexScanNode::execute(EC& ctx)
{
    StorageEngine::PageOps* page_ops = new StorageEngine::PageOps(ctx.schema,ctx.pg_dir,ctx.idx_table);
    Filter filter;
    filter.col_filter = {this->exec_cond};
    ExecResults results = page_ops->IndexTableScan(filter);
    return results;
}

QueryEngine::ExecResults QueryEngine::SeqScanNode::execute(EC& ctx)
{
    StorageEngine::PageOps* page_ops = new StorageEngine::PageOps(ctx.schema,ctx.pg_dir,ctx.idx_table);
    Filter filter;
    filter.col_filter = {this->exec_cond};
    ExecResults results = page_ops->FullTableScan(filter);
    return results;
}