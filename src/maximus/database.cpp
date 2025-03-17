#include <cassert>
#include <maximus/database.hpp>
#include <maximus/error_handling.hpp>
#include <maximus/sql/parser.hpp>
#include <maximus/utils/utils.hpp>

namespace maximus {

Database::Database() {
    db_catalogue_ = make_catalogue(base_path_);
    ctx_          = make_context();
    executor_     = std::make_shared<Executor>(ctx_);
}

Database::Database(std::shared_ptr<DatabaseCatalogue> db_catalogue,
                   std::shared_ptr<MaximusContext> ctx)
        : db_catalogue_(std::move(db_catalogue))
        , ctx_(std::move(ctx))
        , executor_(std::make_shared<Executor>(ctx_)) {
    assert(ctx_);
    assert(ctx_->get_memory_pool());
}

TablePtr Database::query(std::shared_ptr<QueryPlan> &qp) {
    assert(db_catalogue_);
    assert(executor_);
    assert(ctx_);

    executor_->schedule(qp);
    // std::cout << "Query plan scheduled" << std::endl;
    executor_->execute();
    // std::cout << "Query plan executed" << std::endl;
    assert(qp);
    return qp->result();
}

TablePtr Database::query(std::string sql_query) {
    assert(db_catalogue_);
    assert(executor_);
    assert(ctx_);

    std::shared_ptr<QueryPlan> qp;
    auto parser = new Parser(this->schemas_, db_catalogue_, ctx_);
    check_status(parser->query_plan_from_sql(sql_query, qp));
    assert(qp);
    return query(qp);
}

void Database::parse_schema(std::string sql_query) {
    check_status(Parser::schema_from_sql(sql_query, this->schemas_));
}

void Database::schedule(std::shared_ptr<QueryPlan> &query_plan) {
    assert(db_catalogue_);
    assert(executor_);
    assert(ctx_);
    assert(query_plan);

    executor_->schedule(query_plan);
    pending_qp_.push_back(query_plan);
}

std::vector<TablePtr> Database::execute() {
    assert(executor_);
    executor_->execute();

    std::vector<TablePtr> results;
    results.reserve(pending_qp_.size());

    for (auto &qp : pending_qp_) {
        results.push_back(qp->result());
    }

    assert(results.size() == pending_qp_.size());

    pending_qp_.clear();

#ifdef MAXIMUS_WITH_CUDA
    ctx_->tables_pending_copy.clear();
#endif

    return results;
}

std::shared_ptr<MaximusContext> &Database::get_context() {
    return ctx_;
}

const std::shared_ptr<MaximusContext> &Database::get_context() const {
    return ctx_;
}

const std::shared_ptr<DatabaseCatalogue> &Database::get_catalogue() const {
    return db_catalogue_;
}

std::shared_ptr<DatabaseCatalogue> &Database::get_catalogue() {
    return db_catalogue_;
}

std::shared_ptr<Database> make_database(std::shared_ptr<DatabaseCatalogue> db_catalogue) {
    return std::make_shared<Database>(db_catalogue);
}

std::shared_ptr<Database> make_database(std::shared_ptr<DatabaseCatalogue> db_catalogue,
                                        std::shared_ptr<MaximusContext> ctx) {
    return std::make_shared<Database>(db_catalogue, ctx);
}

void Database::load_table(std::string table_name,
                          const std::shared_ptr<Schema> &schema,
                          const std::vector<std::string> &include_columns,
                          const DeviceType &storage_device) {
    auto full_path = db_catalogue_->table_path(table_name);

    tables_[table_name]  = read_table(ctx_, full_path, schema, include_columns, storage_device);
    schemas_[table_name] = schema;
}

DeviceTablePtr Database::get_table(std::string table_name) {
    auto it = tables_.find(table_name);
    if (it == tables_.end()) {
        return {};
    }

    auto &table = it->second;
    auto schema = schemas_.at(table_name);

    if (table.on_cpu()) {
        assert(table.is_table());
        // table.convert_to<TablePtr>(get_context(), schema);
        auto cpu_table = table.as_table();
        assert(cpu_table);
        // we prefer pinned pool for CPU tables, for faster data transfer (CPU->GPU)
        const bool prefer_pinned_pool = ctx_->tables_initially_pinned;

        const bool as_single_chunk = ctx_->tables_initially_as_single_chunk;
        return DeviceTablePtr(cpu_table->clone(prefer_pinned_pool, as_single_chunk));
    }
    if (table.on_gpu()) {
#ifdef MAXIMUS_WITH_CUDA
        table.convert_to<GTablePtr>(get_context(), schema);
        auto gpu_table = table.as_gtable()->clone();
        assert(gpu_table);
        assert(tables_[table_name].is_gtable());
        return DeviceTablePtr(std::move(gpu_table));
#else
        throw std::runtime_error("Maximus must be built with the GPU support");
#endif
    }
    return {};
}

}  // namespace maximus
