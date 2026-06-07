#include "../headers/InternalQuery.h"
#include "../headers/Planner.h"
#include <memory>



QueryEngine::PlanType QueryEngine::Planner::GeneratePlan(InternalQuery::Query* query)
{
    QueryEngine::PlanType p_type;
    if(query->qtype == InternalQuery::QueryType::SELECT_QUERY)
    {
        auto _query = static_cast<InternalQuery::SelectQuery*>(query);
        p_type = PlanSelectQuery(_query);
    }
    else if(query->qtype == InternalQuery::QueryType::UPDATE_QUERY)
    {
        auto _query = static_cast<InternalQuery::UpdateQuery*>(query);
        p_type = PlanUpdateQuery(_query);
    }
    else if(query->qtype == InternalQuery::QueryType::DELETE_QUERY)
    {
        auto _query = static_cast<InternalQuery::DeleteQuery*>(query);
        p_type = PlanDeleteQuery(_query);
    }
    else if (query->qtype == InternalQuery::QueryType::INSERT_QUERY) {
        auto _query = static_cast<InternalQuery::InsertQuery*>(query);
        p_type = PlanInsertQuery(_query);
    }
    else if(query->qtype == InternalQuery::QueryType::INDEX_QUERY)
    {
        auto _query = static_cast<InternalQuery::IndexQuery*>(query);
        p_type = PlanIndexQuery(_query);
    }
    else {
        logger->logError({"Query Type Dosen't exists"});
        p_type = std::make_unique<QueryEngine::InvalidNode>();
    }
    return p_type;
}

QueryEngine::ExecResults QueryEngine::Planner::ExecutePlan(QueryEngine::PlanType& plan)
{
    ExecutionContext ctx{db_path, table_path,tSchema, pg_dir, idx_table};
    ExecResults results;
    std::visit([&ctx, &results](auto& ptr) {
        /* call the execute on the plan --- execute implementation will vary based on ExecNode */
        results = ptr->execute(ctx);
    }, plan);
    return results;
}
