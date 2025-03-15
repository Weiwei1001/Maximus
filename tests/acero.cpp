#include <arrow/acero/hash_join.h>
#include <arrow/acero/options.h>
#include <arrow/acero/util.h>
#include <arrow/result.h>
#include <arrow/type.h>
#include <gtest/gtest.h>

#include <maximus/database.hpp>
#include <maximus/operators/acero/dummy_node.hpp>
#include <maximus/utils/arrow_helpers.hpp>

using namespace maximus;

namespace test {

namespace cp = ::arrow::compute;
namespace ac = ::arrow::acero;

TEST(acero, CrossJoin) {
    std::cout << "" << std::endl;
    std::cout << "=============================" << std::endl;
    std::cout << "CROSS-JOIN" << std::endl;
    std::cout << "" << std::endl;

    // field and schema creation

    // left side
    auto lfield = {arrow::field("a", arrow::utf8())};

    // right side
    auto rfield1 = {arrow::field("b", arrow::int8())};
    auto rfield2 = {arrow::field("c", arrow::utf8())};
    auto rfield3 = {arrow::field("d", arrow::float32())};
    auto rfield4 = {arrow::field("e", arrow::int8())};

    // recordbatch creation

    auto context = maximus::make_context();

    // left side
    auto left_rbatch = maximus::generate_batch(lfield, 5, 0, context->get_memory_pool());

    // right side
    auto rbatch1 = maximus::generate_batch(rfield1, 3, 0, context->get_memory_pool());
    auto rbatch2 = maximus::generate_batch(rfield2, 2, 1, context->get_memory_pool());
    auto rbatch3 = maximus::generate_batch(rfield3, 4, 0, context->get_memory_pool());
    auto rbatch4 = maximus::generate_batch(rfield4, 5, 2, context->get_memory_pool());

    // vector of the right-side recordbatches and a NULL pointer
    std::vector<std::shared_ptr<arrow::RecordBatch>> right_rbatches = {
        rbatch1, rbatch2, rbatch3, rbatch4};

    // get the number of rows from the left side
    int num_rows_left       = left_rbatch->num_rows();
    int size_right_rbatches = right_rbatches.size();

    // create a vector of record batches with size=num_rows
    std::vector<std::shared_ptr<arrow::RecordBatch>> conc_RBs;

    for (int j = 0; j < size_right_rbatches; j++) {
        auto right = right_rbatches[j];

        // get the number of rows from the right side
        int num_rows_right = right->num_rows();

        for (int i = 0; i < num_rows_right; i++) {
            // Extract the i-th row from the given RecordBatch
            std::vector<std::shared_ptr<arrow::Array>> first_row_arrays;
            for (const auto& array : right->columns()) {
                first_row_arrays.push_back(array->Slice(i, 1));
            }

            // Repeat the first row num_rows_left-times
            std::vector<std::shared_ptr<arrow::Array>> repeated_arrays;
            for (const auto& array : first_row_arrays) {
                std::vector<std::shared_ptr<arrow::Array>> temp_arrays(num_rows_left, array);
                auto maybe_result = arrow::Concatenate(temp_arrays, arrow::default_memory_pool());
                if (!maybe_result.ok()) {
                    // Handle error
                }
                repeated_arrays.push_back(maybe_result.ValueOrDie());
            }

            // Create a new RecordBatch with the repeated first row
            auto new_batch =
                arrow::RecordBatch::Make(right->schema(), num_rows_left, repeated_arrays);

            // Concatenate the two RecordBatches
            std::vector<std::shared_ptr<arrow::Array>> columns = left_rbatch->columns();
            const std::vector<std::shared_ptr<arrow::Array>>& right_columns = new_batch->columns();
            columns.insert(columns.end(), right_columns.begin(), right_columns.end());

            std::vector<std::shared_ptr<arrow::Field>> fields = left_rbatch->schema()->fields();
            const std::vector<std::shared_ptr<arrow::Field>>& right_fields =
                new_batch->schema()->fields();
            fields.insert(fields.end(), right_fields.begin(), right_fields.end());

            conc_RBs.push_back(arrow::RecordBatch::Make(
                arrow::schema(std::move(fields)), num_rows_left, std::move(columns)));
        }
    }


    std::cout << "Left:" << std::endl;
    maximus::TableBatch batchLeft(context, left_rbatch);
    batchLeft.print();
    std::cout << "" << std::endl;

    std::cout << "Right:" << std::endl;

    //printing all entries of the right-side recordbatches
    for (int j = 0; j < size_right_rbatches; j++) {
        maximus::TableBatch batchRight(context, right_rbatches[j]);
        batchRight.print();
        std::cout << "" << std::endl;
    }
    std::cout << "" << std::endl;

    std::cout << "Cross-Join Left(blocking) and Right(streaming):" << std::endl;

    // print all the entries in conc_RBs
    for (size_t i = 0; i < conc_RBs.size(); i++) {
        std::cout << i << std::endl;
        maximus::TableBatch batch(context, conc_RBs[i]);
        batch.print();
        std::cout << "" << std::endl;
    }

    std::cout << "=============================" << std::endl;
    std::cout << "\n" << std::endl;
}

TEST(acero, DeclarationToReader) {
    auto context = make_context();
    auto lfields = {arrow::field("a", arrow::int32())};
    auto lschema = arrow::schema(lfields);

    auto rfields = {arrow::field("b", arrow::int32()), arrow::field("c", arrow::binary())};
    auto rschema = arrow::schema(rfields);

    std::shared_ptr<arrow::RecordBatch> lbatch =
        maximus::generate_batch(lfields, 10, 0, context->get_memory_pool());

    std::shared_ptr<arrow::RecordBatch> rbatch =
        maximus::generate_batch(rfields, 10, 0, context->get_memory_pool());

    std::shared_ptr<arrow::ExecBatch> lexec_batch;
    auto ltable_batch = std::make_shared<TableBatch>(context, lbatch);
    maximus::check_status(ltable_batch->to_exec_batch(lexec_batch));

    std::shared_ptr<arrow::ExecBatch> rexec_batch;
    auto rtable_batch = std::make_shared<TableBatch>(context, rbatch);
    maximus::check_status(rtable_batch->to_exec_batch(rexec_batch));

    rexec_batch->index = 0;
    lexec_batch->index = 0;

    std::cout << "Left data " << lexec_batch->ToString() << std::endl;
    std::cout << "Right data " << rexec_batch->ToString() << std::endl;

    arrow::PushGenerator<std::optional<arrow::ExecBatch>> left_gen;
    arrow::PushGenerator<std::optional<arrow::ExecBatch>> right_gen;

    auto lsource = arrow::acero::Declaration(
        "source", arrow::acero::SourceNodeOptions{lschema, left_gen}, "left_source");
    auto rsource = arrow::acero::Declaration(
        "source", arrow::acero::SourceNodeOptions{rschema, right_gen}, "right_source");

    std::vector<arrow::FieldRef> left_keys, right_keys;
    left_keys.emplace_back("a");
    right_keys.emplace_back("b");

    auto options = arrow::acero::HashJoinNodeOptions{
        left_keys,
        right_keys,
    };

    auto join =
        arrow::acero::Declaration("hashjoin", {lsource, rsource}, std::move(options), "join");

    auto maybe_reader = arrow::acero::DeclarationToReader(join, true, context->get_memory_pool());
    if (!maybe_reader.ok()) {
        maximus::check_status(maybe_reader.status());
    }
    auto reader = std::move(maybe_reader.ValueOrDie());

    auto right_producer = right_gen.producer();
    right_producer.Push(std::move(*rexec_batch));
    right_producer.Close();

    auto left_producer = left_gen.producer();
    left_producer.Push(*lexec_batch);
    left_producer.Push(*lexec_batch);
    left_producer.Push(*lexec_batch);
    left_producer.Close();

    bool has_next = true;
    while (has_next) {
        auto maybe_batch = reader->Next();
        if (!maybe_batch.ok()) {
            std::cout << maybe_batch.status().message() << std::endl;
            check_status(maybe_batch.status());
            has_next = false;
        }
        auto batch = maybe_batch.ValueOrDie();
        if (batch) {
            std::cout << "RecordBatchReader: " << batch->ToString() << std::endl;
        } else {
            has_next = false;
        }
    }
}

TEST(acero, MakeGeneratorReader) {
    std::cout << "MAKE GENERATOR READER CASE WITH ACERO..." << std::endl;
    auto context = make_context();
    auto lfields = {arrow::field("a", arrow::int32())};
    auto lschema = arrow::schema(lfields);

    auto rfields = {arrow::field("b", arrow::int32()), arrow::field("c", arrow::binary())};
    auto rschema = arrow::schema(rfields);

    auto exec_plan = context->get_mini_exec_plan();

    arrow::PushGenerator<std::optional<arrow::ExecBatch>> left_gen;
    arrow::PushGenerator<std::optional<arrow::ExecBatch>> right_gen;
    arrow::AsyncGenerator<std::optional<arrow::ExecBatch>> sink_gen;

    auto lsource = arrow::acero::Declaration(
        "source", arrow::acero::SourceNodeOptions{lschema, left_gen}, "left_source");
    auto rsource = arrow::acero::Declaration(
        "source", arrow::acero::SourceNodeOptions{rschema, right_gen}, "right_source");

    std::vector<arrow::FieldRef> left_keys, right_keys;
    left_keys.emplace_back("a");
    right_keys.emplace_back("b");

    auto options = arrow::acero::HashJoinNodeOptions{
        left_keys,
        right_keys,
    };

    auto join =
        arrow::acero::Declaration("hashjoin", {lsource, rsource}, std::move(options), "join");

    auto sink =
        arrow::acero::Declaration("sink", {join}, arrow::acero::SinkNodeOptions{&sink_gen}, "sink");

    auto maybe_sink_node = sink.AddToPlan(exec_plan.get());
    auto sink_node       = maybe_sink_node.ValueOrDie();

    std::cout << "plan = " << exec_plan->ToString() << std::endl;

    check_status(exec_plan->Validate());
    exec_plan->StartProducing();

    auto output_schema = sink_node->inputs()[0]->output_schema();
    std::cout << "output schema = " << output_schema->ToString() << std::endl;

    std::shared_ptr<arrow::RecordBatch> lbatch =
        maximus::generate_batch(lfields, 10, 0, context->get_memory_pool());

    std::shared_ptr<arrow::RecordBatch> rbatch =
        maximus::generate_batch(rfields, 10, 0, context->get_memory_pool());

    std::shared_ptr<arrow::ExecBatch> lexec_batch;
    auto ltable_batch = std::make_shared<TableBatch>(context, lbatch);
    check_status(ltable_batch->to_exec_batch(lexec_batch));

    std::shared_ptr<arrow::ExecBatch> rexec_batch;
    auto rtable_batch = std::make_shared<TableBatch>(context, rbatch);
    check_status(rtable_batch->to_exec_batch(rexec_batch));

    rexec_batch->index = 0;
    lexec_batch->index = 0;

    std::cout << "Left data " << lexec_batch->ToString() << std::endl;
    std::cout << "Right data " << rexec_batch->ToString() << std::endl;

    auto right_producer = right_gen.producer();
    right_producer.Push(std::move(*rexec_batch));
    right_producer.Close();

    auto left_producer = left_gen.producer();
    left_producer.Push(*lexec_batch);
    left_producer.Push(*lexec_batch);
    left_producer.Push(*lexec_batch);
    left_producer.Close();

    auto batch_reader =
        arrow::acero::MakeGeneratorReader(output_schema, sink_gen, context->get_memory_pool());

    bool has_next = true;
    while (has_next) {
        auto maybe_batch = batch_reader->Next();
        if (!maybe_batch.ok()) {
            std::cout << maybe_batch.status().message() << std::endl;
            check_status(maybe_batch.status());
            has_next = false;
        }
        auto batch = maybe_batch.ValueOrDie();
        if (batch) {
            std::cout << "RecordBatchReader: " << batch->ToString() << std::endl;
        } else {
            has_next = false;
        }
    }

    exec_plan->StopProducing();

    std::cout << "exec plan finished" << std::endl;
}

TEST(acero, MakeGeneratorIterator) {
    auto context = make_context();
    auto lfields = {arrow::field("a", arrow::int32())};
    auto lschema = arrow::schema(lfields);

    auto rfields = {arrow::field("b", arrow::int32()), arrow::field("c", arrow::binary())};
    auto rschema = arrow::schema(rfields);

    auto exec_plan = context->get_mini_exec_plan();

    arrow::PushGenerator<std::optional<arrow::ExecBatch>> left_gen;
    arrow::PushGenerator<std::optional<arrow::ExecBatch>> right_gen;
    arrow::AsyncGenerator<std::optional<arrow::ExecBatch>> sink_gen;

    auto lsource = arrow::acero::Declaration(
        "source", arrow::acero::SourceNodeOptions{lschema, left_gen}, "left_source");
    auto rsource = arrow::acero::Declaration(
        "source", arrow::acero::SourceNodeOptions{rschema, right_gen}, "right_source");

    std::vector<arrow::FieldRef> left_keys, right_keys;
    left_keys.emplace_back("a");
    right_keys.emplace_back("b");

    auto options = arrow::acero::HashJoinNodeOptions{
        left_keys,
        right_keys,
    };

    auto join =
        arrow::acero::Declaration("hashjoin", {lsource, rsource}, std::move(options), "join");

    auto sink =
        arrow::acero::Declaration("sink", {join}, arrow::acero::SinkNodeOptions{&sink_gen}, "sink");

    auto maybe_sink_node = sink.AddToPlan(exec_plan.get());
    auto sink_node       = maybe_sink_node.ValueOrDie();

    std::cout << "plan = " << exec_plan->ToString() << std::endl;

    check_status(exec_plan->Validate());
    exec_plan->StartProducing();

    auto output_schema = sink_node->inputs()[0]->output_schema();
    std::cout << "output schema = " << output_schema->ToString() << std::endl;

    std::shared_ptr<arrow::RecordBatch> lbatch =
        maximus::generate_batch(lfields, 10, 0, context->get_memory_pool());

    std::shared_ptr<arrow::RecordBatch> rbatch =
        maximus::generate_batch(rfields, 10, 0, context->get_memory_pool());

    std::shared_ptr<arrow::ExecBatch> lexec_batch;
    auto ltable_batch = std::make_shared<TableBatch>(context, lbatch);
    check_status(ltable_batch->to_exec_batch(lexec_batch));

    std::shared_ptr<arrow::ExecBatch> rexec_batch;
    auto rtable_batch = std::make_shared<TableBatch>(context, rbatch);
    check_status(rtable_batch->to_exec_batch(rexec_batch));

    rexec_batch->index = 0;
    lexec_batch->index = 0;

    std::cout << "Left data " << lexec_batch->ToString() << std::endl;
    std::cout << "Right data " << rexec_batch->ToString() << std::endl;

    auto right_producer = right_gen.producer();
    right_producer.Push(std::move(*rexec_batch));
    right_producer.Close();

    auto left_producer = left_gen.producer();
    left_producer.Push(*lexec_batch);
    left_producer.Push(*lexec_batch);
    left_producer.Push(*lexec_batch);
    left_producer.Close();

    auto iterator = arrow::MakeGeneratorIterator(sink_gen);

    bool has_next = true;
    while (has_next) {
        auto maybe_result = iterator.Next();
        if (!maybe_result.ok()) {
            std::cout << maybe_result.status().message() << std::endl;
            check_status(maybe_result.status());
            has_next = false;
        }
        auto result = maybe_result.ValueOrDie();
        if (result.has_value())
            std::cout << "Iterator Result = " << result.value().ToString() << std::endl;
        else
            has_next = false;
    }

    exec_plan->StopProducing();

    std::cout << "exec plan finished" << std::endl;
}

TEST(acero, CollectAsyncGenerator) {
    auto context = make_context();
    auto lfields = {arrow::field("a", arrow::int32())};
    auto lschema = arrow::schema(lfields);

    auto rfields = {arrow::field("b", arrow::int32()), arrow::field("c", arrow::binary())};
    auto rschema = arrow::schema(rfields);

    auto exec_plan = context->get_mini_exec_plan();

    arrow::PushGenerator<std::optional<arrow::ExecBatch>> left_gen;
    arrow::PushGenerator<std::optional<arrow::ExecBatch>> right_gen;
    arrow::AsyncGenerator<std::optional<arrow::ExecBatch>> sink_gen;

    auto lsource = arrow::acero::Declaration(
        "source", arrow::acero::SourceNodeOptions{lschema, left_gen}, "left_source");
    auto rsource = arrow::acero::Declaration(
        "source", arrow::acero::SourceNodeOptions{rschema, right_gen}, "right_source");

    std::vector<arrow::FieldRef> left_keys, right_keys;
    left_keys.emplace_back("a");
    right_keys.emplace_back("b");

    auto options = arrow::acero::HashJoinNodeOptions{
        left_keys,
        right_keys,
    };

    auto join =
        arrow::acero::Declaration("hashjoin", {lsource, rsource}, std::move(options), "join");

    auto sink =
        arrow::acero::Declaration("sink", {join}, arrow::acero::SinkNodeOptions{&sink_gen}, "sink");

    auto maybe_sink_node = sink.AddToPlan(exec_plan.get());
    auto sink_node       = maybe_sink_node.ValueOrDie();

    std::cout << "plan = " << exec_plan->ToString() << std::endl;

    check_status(exec_plan->Validate());
    exec_plan->StartProducing();

    auto output_schema = sink_node->inputs()[0]->output_schema();
    std::cout << "output schema = " << output_schema->ToString() << std::endl;

    std::shared_ptr<arrow::RecordBatch> lbatch =
        maximus::generate_batch(lfields, 10, 0, context->get_memory_pool());

    std::shared_ptr<arrow::RecordBatch> rbatch =
        maximus::generate_batch(rfields, 10, 0, context->get_memory_pool());

    std::shared_ptr<arrow::ExecBatch> lexec_batch;
    auto ltable_batch = std::make_shared<TableBatch>(context, lbatch);
    check_status(ltable_batch->to_exec_batch(lexec_batch));

    std::shared_ptr<arrow::ExecBatch> rexec_batch;
    auto rtable_batch = std::make_shared<TableBatch>(context, rbatch);
    check_status(rtable_batch->to_exec_batch(rexec_batch));

    rexec_batch->index = 0;
    lexec_batch->index = 0;

    std::cout << "Left data " << lexec_batch->ToString() << std::endl;
    std::cout << "Right data " << rexec_batch->ToString() << std::endl;

    auto right_producer = right_gen.producer();
    right_producer.Push(std::move(*rexec_batch));
    right_producer.Close();

    auto left_producer = left_gen.producer();
    left_producer.Push(*lexec_batch);
    left_producer.Push(*lexec_batch);
    left_producer.Push(*lexec_batch);
    left_producer.Close();

    auto future = arrow::CollectAsyncGenerator(sink_gen);

    auto result = future.MoveResult().ValueOrDie();
    for (const auto& batch : result) {
        std::cout << "Result: " << batch.value().ToString() << std::endl;
    }

    exec_plan->StopProducing();

    std::cout << "exec plan finished" << std::endl;
}

TEST(acero, VisitAsyncGenerator) {
    auto context = make_context();
    auto lfields = {arrow::field("a", arrow::int32())};
    auto lschema = arrow::schema(lfields);

    auto rfields = {arrow::field("b", arrow::int32()), arrow::field("c", arrow::binary())};
    auto rschema = arrow::schema(rfields);

    auto exec_plan = context->get_mini_exec_plan();

    arrow::PushGenerator<std::optional<arrow::ExecBatch>> left_gen;
    arrow::PushGenerator<std::optional<arrow::ExecBatch>> right_gen;
    arrow::AsyncGenerator<std::optional<arrow::ExecBatch>> sink_gen;

    auto lsource = arrow::acero::Declaration(
        "source", arrow::acero::SourceNodeOptions{lschema, left_gen}, "left_source");
    auto rsource = arrow::acero::Declaration(
        "source", arrow::acero::SourceNodeOptions{rschema, right_gen}, "right_source");

    std::vector<arrow::FieldRef> left_keys, right_keys;
    left_keys.emplace_back("a");
    right_keys.emplace_back("b");

    auto options = arrow::acero::HashJoinNodeOptions{
        left_keys,
        right_keys,
    };

    auto join =
        arrow::acero::Declaration("hashjoin", {lsource, rsource}, std::move(options), "join");

    auto sink =
        arrow::acero::Declaration("sink", {join}, arrow::acero::SinkNodeOptions{&sink_gen}, "sink");

    auto maybe_sink_node = sink.AddToPlan(exec_plan.get());
    auto sink_node       = maybe_sink_node.ValueOrDie();

    std::cout << "plan = " << exec_plan->ToString() << std::endl;

    check_status(exec_plan->Validate());
    exec_plan->StartProducing();

    auto output_schema = sink_node->inputs()[0]->output_schema();
    std::cout << "output schema = " << output_schema->ToString() << std::endl;

    std::shared_ptr<arrow::RecordBatch> lbatch =
        maximus::generate_batch(lfields, 10, 0, context->get_memory_pool());

    std::shared_ptr<arrow::RecordBatch> rbatch =
        maximus::generate_batch(rfields, 10, 0, context->get_memory_pool());

    std::shared_ptr<arrow::ExecBatch> lexec_batch;
    auto ltable_batch = std::make_shared<TableBatch>(context, lbatch);
    check_status(ltable_batch->to_exec_batch(lexec_batch));

    std::shared_ptr<arrow::ExecBatch> rexec_batch;
    auto rtable_batch = std::make_shared<TableBatch>(context, rbatch);
    check_status(rtable_batch->to_exec_batch(rexec_batch));

    rexec_batch->index = 0;
    lexec_batch->index = 0;

    std::cout << "Left data " << lexec_batch->ToString() << std::endl;
    std::cout << "Right data " << rexec_batch->ToString() << std::endl;

    auto right_producer = right_gen.producer();
    right_producer.Push(std::move(*rexec_batch));
    right_producer.Close();

    auto left_producer = left_gen.producer();
    left_producer.Push(*lexec_batch);
    left_producer.Push(*lexec_batch);
    left_producer.Push(*lexec_batch);
    left_producer.Close();

    auto finished =
        arrow::VisitAsyncGenerator(sink_gen, [&](const std::optional<arrow::ExecBatch>& batch) {
            if (batch.has_value()) {
                std::cout << "Visitor Result: " << batch.value().ToString() << std::endl;
            }
            return arrow::Status::OK();
        });

    finished.Wait();

    exec_plan->StopProducing();

    std::cout << "exec plan finished" << std::endl;
}

TEST(acero, filter) {
    auto context = make_context();
    auto fields  = {arrow::field("a", arrow::int32())};
    auto schema  = arrow::schema(fields);

    auto exec_plan = context->get_mini_exec_plan();
    auto source    = acero::MakeDummySource(exec_plan.get(), schema, "source");

    auto maybe_filter = arrow::acero::MakeExecNode(
        "filter",
        exec_plan.get(),
        {source},
        arrow::acero::FilterNodeOptions{
            arrow::compute::less(arrow::compute::field_ref("a"), arrow::compute::literal(4))});
    if (!maybe_filter.ok()) {
        check_status(maybe_filter.status());
        return;
    }

    auto filter = maybe_filter.ValueOrDie();

    auto sink = acero::MakeDummySink(exec_plan.get(), {filter}, "sink");

    exec_plan->StartProducing();

    std::shared_ptr<arrow::RecordBatch> batch =
        maximus::generate_batch(fields, 1000, 0, context->get_memory_pool());

    std::shared_ptr<arrow::ExecBatch> exec_batch;
    auto table_batch = std::make_shared<TableBatch>(context, batch);
    check_status(table_batch->to_exec_batch(exec_batch));

    check_status(filter->InputReceived(source, std::move(*exec_batch)));
    check_status(filter->InputFinished(source, 1));

    exec_plan->StopProducing();
    exec_plan->finished().Wait();
}
}  // namespace test
