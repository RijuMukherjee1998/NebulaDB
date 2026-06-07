#pragma once

#include <memory>
#include "InternalQuery.h"
#include "Executor.h"
#include "PageDirectory.h"
#include "Schema.h"
#include "Logger.h"


namespace QueryEngine {
    using PlanType = std::variant<std::unique_ptr<QueryEngine::SelectNode>, std::unique_ptr<QueryEngine::UpdateNode>,
                std::unique_ptr<QueryEngine::DeleteNode>, std::unique_ptr<QueryEngine::InsertNode>, std::unique_ptr<QueryEngine::IndexNode>
                ,std::unique_ptr<QueryEngine::InvalidNode>>;

    class Planner {
        Utils::Logger* logger = nullptr;
        std::filesystem::path db_path;
        std::filesystem::path table_path;
        Schema* tSchema;
        StorageEngine::PageDirectory* pg_dir;
        IndexTableType* idx_table;
        public:
            Planner(const std::filesystem::path& db_path, const std::filesystem::path& table_path, Schema* schema, StorageEngine::PageDirectory* pg_dir, IndexTableType* it)
            {
                this->db_path = db_path;
                this->table_path = table_path;
                tSchema = schema;
                this->pg_dir = pg_dir;
                idx_table = it;
                logger = Utils::Logger::getInstance();
            }
            PlanType GeneratePlan(InternalQuery::Query* query);
            ExecResults ExecutePlan(PlanType& plan);
        private:
            bool isColumnIndexed(uint16_t col_idx)
            {
                return tSchema->isColIndexed(col_idx);
            }
            void PlanAndQuery(InternalQuery::AndQuery& a_query, std::unique_ptr<QueryEngine::FilterNode>& f_node)
            {
                for(auto& cond : a_query.conditions) {
                    if(isColumnIndexed(cond.col_idx))
                    {
                        std::unique_ptr<QueryEngine::ScanNode> is_node = std::make_unique<QueryEngine::IndexScanNode>();
                        is_node->exec_cond = ExecCondition();
                        is_node->exec_cond.col_idx = cond.col_idx;
                        is_node->exec_cond.op = (ExecConds)cond.op;
                        is_node->exec_cond.fil_type = (ExecCondition::Filtype)cond.fil_type;
                        is_node->exec_cond.value = cond.value;
                        is_node->exec_cond.range = cond.range;
                        f_node->scanNodes.emplace_back(std::move(is_node));
                    } else {
                        std::unique_ptr<QueryEngine::ScanNode> ss_node = std::make_unique<QueryEngine::SeqScanNode>();
                        ss_node->exec_cond = ExecCondition();
                        ss_node->exec_cond.col_idx = cond.col_idx;
                        ss_node->exec_cond.op = (ExecConds)cond.op;
                        ss_node->exec_cond.fil_type = (ExecCondition::Filtype)cond.fil_type;
                        ss_node->exec_cond.value = cond.value;
                        ss_node->exec_cond.range = cond.range;
                        f_node->scanNodes.emplace_back(std::move(ss_node));
                    }
                }
            }

            void PlanORQuery(InternalQuery::OrQuery& o_query, std::unique_ptr<UnionNode>& u_node)
            {
                for(auto& andq : o_query.and_groups)
                {
                    std::unique_ptr<FilterNode> f_node = std::make_unique<FilterNode>();
                    PlanAndQuery(andq, f_node);
                    u_node->filterNodes.emplace_back(std::move(f_node));
                }
            }

            PlanType PlanSelectQuery(InternalQuery::SelectQuery* selectQuery)
            {
                std::unique_ptr<SelectNode> select_node = std::make_unique<SelectNode>();
                if (!selectQuery->predicate.and_groups.empty())
                {
                    std::unique_ptr<UnionNode> u_node = std::make_unique<UnionNode>();
                    PlanORQuery(selectQuery->predicate, u_node);
                    select_node->predicate = std::move(u_node);
                }
                select_node->projections = selectQuery->projection;
                return select_node;
            }

            PlanType PlanUpdateQuery(InternalQuery::UpdateQuery* updateQuery)
            {
                std::unique_ptr<UnionNode> u_node = std::make_unique<UnionNode>();
                PlanORQuery(updateQuery->predicate, u_node);
                std::unique_ptr<UpdateNode> update_node = std::make_unique<UpdateNode>();
                update_node->predicate = std::move(u_node);
                update_node->updates = updateQuery->updates;
                return update_node;
            }

            PlanType PlanDeleteQuery(InternalQuery::DeleteQuery* deleteQuery)
            {
                std::unique_ptr<UnionNode> u_node = std::make_unique<UnionNode>();
                PlanORQuery(deleteQuery->predicate, u_node);
                std::unique_ptr<DeleteNode> delete_node = std::make_unique<DeleteNode>();
                delete_node->predicate = std::move(u_node);
                return delete_node;
            }

            PlanType PlanInsertQuery(InternalQuery::InsertQuery* insertQuery)
            {
                std::unique_ptr<InsertNode> insert_node = std::make_unique<InsertNode>();
                insert_node->values = insertQuery->values;
                return insert_node;
            }

            PlanType PlanIndexQuery(InternalQuery::IndexQuery* indexQuery)
            {
                std::unique_ptr<IndexNode> index_node = std::make_unique<IndexNode>();
                index_node->col_id = indexQuery->col_id;
                return index_node;
            }
    };
}
