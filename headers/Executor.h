#pragma once

#include <cstdint>
#include <vector>
#include <memory>
#include <unordered_map>
#include "Indexer.h"
#include "Schema.h"
#include "PageDirectory.h"
#include "InternalStructs.h"
#include "constants.h"


namespace  QueryEngine{

/* 
    * Plan → Create the ExecTree
    * Travel the ExecTree and run the executor
    * Executor → (produces RowIDs  or Inserts or CreateIndex) --> by executing each ExecNodes
    * Then:
    * for each RowID:
    *    execute(select/update/delete)(RowID) 
*/
using IndexTableType = std::unordered_map<uint16_t, std::unique_ptr<StorageEngine::Indexer<variant_data_t,std::pair<PAGE_ID_TYPE,SLOT_ID_TYPE>>>>;
struct ExecutionContext {
    Schema* schema;
    StorageEngine::PageDirectory* pg_dir;
    IndexTableType* idx_table;
    // later: txn, buffer pool, catalog, etc.
};

class ExecNode {
public:
    virtual  ExecResults execute(ExecutionContext& ctx) = 0;
    virtual ~ExecNode() = default;
};
class FilterNode;
class ScanNode;

/* All of theses forms the  filter nodes */
class UnionNode : public ExecNode {
    public:
        std::vector<std::unique_ptr<FilterNode>> filterNodes;
        ExecResults execute(ExecutionContext& ctx) override;
};

class FilterNode : public ExecNode {
    public:
        std::vector<std::unique_ptr<ScanNode>> scanNodes;
        ExecResults execute(ExecutionContext& ctx) override;
};
class ScanNode : public ExecNode {
    public:
        ExecCondition exec_cond;
        enum ScanType 
        {
            IndexScan = 0,
            SeqScan
        };
        ScanType type;
};
class IndexScanNode : public ScanNode {
    public:
        ScanType type = ScanType::IndexScan;
        ExecResults execute(ExecutionContext& ctx) override;
};
class SeqScanNode : public ScanNode {
    public:
        ScanType type = ScanType::SeqScan;
        ExecResults execute(ExecutionContext& ctx) override;
};


/* These needs the outut from filter nodes for further ops */
class DeleteNode : public ExecNode {
    public:
        std::unique_ptr<UnionNode> predicate = nullptr; 
        ExecResults execute(ExecutionContext& ctx) override;
};

class UpdateNode : public ExecNode {
    public:
        std::unique_ptr<UnionNode> predicate = nullptr;
        std::unordered_map<COL_ID_TYPE, variant_data_t> updates;
        ExecResults execute(ExecutionContext& ctx) override;
};

class SelectNode : public ExecNode {
    public:
        std::unique_ptr<UnionNode> predicate = nullptr;
        std::vector<COL_ID_TYPE> projections;
        ExecResults execute(ExecutionContext& ctx) override;
};

/* Directly these nodes execute from the query plan */
class InsertNode : public ExecNode {
    public:
        std::vector<variant_data_t> values;
        ExecResults execute(ExecutionContext& ctx) override;
};

class IndexNode : public ExecNode {
    public:
        COL_ID_TYPE col_id;
        ExecResults execute(ExecutionContext& ctx) override;
}; 

class InvalidNode : public ExecNode{
    public:
    ExecResults execute(ExecutionContext& ctx) override;
};

}