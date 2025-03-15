#include <gtest/gtest.h>

#include <maximus/context.hpp>
#include <maximus/dag/query_plan.hpp>
#include <maximus/operators/acero/group_by_operator.hpp>
#include <maximus/types/aggregate.hpp>
#include <maximus/types/table_batch.hpp>

namespace test {

TEST(Operators, AceroGroupBy) {
    // ===============================================
    //     CREATING THE INPUT SCHEMA
    // ===============================================
    // a source node generating random tuples
    auto fields = {
        arrow::field("key", arrow::utf8()),    //  a string
        arrow::field("value", arrow::int32())  // an integer
    };
    auto input_schema = std::make_shared<maximus::Schema>(fields);

    // ===============================================
    //     CREATING THE AGGREGATES
    // ===============================================
    auto arrow_aggregate =
        std::make_shared<arrow::compute::Aggregate>("hash_sum", "value", "value_sum");

    auto aggregate = std::make_shared<maximus::Aggregate>(std::move(arrow_aggregate));

    // ===============================================
    //     CREATING THE GROUP KEYS
    // ===============================================
    std::vector<arrow::FieldRef> group_keys                     = {"key"};
    std::vector<std::shared_ptr<maximus::Aggregate>> aggregates = {std::move(aggregate)};
    // std::cout << "Group keys[0] = " << group_keys[0] << std::endl;

    // ===============================================
    //     WRAPPING INSIDE THE GROUP BY PROPERTIES
    // ===============================================
    auto group_by_properties =
        std::make_shared<maximus::GroupByProperties>(std::move(group_keys), std::move(aggregates));
    // std::cout << "Group by properties = \n" << group_by_properties->to_string() << std::endl;

    // ===============================================
    //     CREATING THE OPERATOR
    // ===============================================
    auto context           = maximus::make_context();
    auto group_by_operator = std::make_shared<maximus::acero::GroupByOperator>(
        context, input_schema, std::move(group_by_properties));

    group_by_operator->next_op_type     = maximus::PhysicalOperatorType::TABLE_SINK;
    group_by_operator->next_engine_type = maximus::EngineType::NATIVE;

    std::cout << "Finished creating the operator" << std::endl;
    std::cout << "operator = \n" << group_by_operator->to_string() << std::endl;

    // ===============================================
    //     GENERATE A RANDOM INPUT BATCH
    // ===============================================
    maximus::TableBatchPtr batch;
    auto status = maximus::TableBatch::from_json(context,
                                                 input_schema,
                                                 {R"([
            ["x", 1],
            ["y", 2],
            ["y", 3],
            ["z", 4],
            ["z", 5]
        ])"},
                                                 batch);
    check_status(status);
    std::cout << "===========================" << std::endl;
    std::cout << "     The input batch =     " << std::endl;
    std::cout << "===========================" << std::endl;
    batch->print();

    // ===============================================
    //     PUSH THE BATCH TO THE OPERATOR
    // ===============================================
    group_by_operator->add_input(maximus::DeviceTablePtr(std::move(batch)), 0);
    group_by_operator->no_more_input(0);

    // ===============================================
    //     PRINT THE OUTPUT BATCHES
    // ===============================================
    int num_batches = 0;
    maximus::DeviceTablePtr output_batch;
    while (group_by_operator->has_more_batches(true)) {
        output_batch = group_by_operator->export_next_batch();
        num_batches++;
    }
    EXPECT_EQ(num_batches, 1);

    // ===============================================
    //     EXPECTED OUTPUT SCHEMA
    // ===============================================
    // note that the value_sum column has a different name than the input schema
    auto expected_fields = {
        arrow::field("key", arrow::utf8()),        //  a string
        arrow::field("value_sum", arrow::int32())  // an integer
    };
    auto output_schema = std::make_shared<maximus::Schema>(expected_fields);

    // ===============================================
    //     EXPECTED OUTPUT BATCH
    // ===============================================
    maximus::TableBatchPtr expected;
    status = maximus::TableBatch::from_json(context,
                                            output_schema,
                                            {R"([
            ["x", 1],
            ["y", 5],
            ["z", 9]
        ])"},
                                            expected);

    check_status(status);
    std::cout << "===========================" << std::endl;
    std::cout << "     The output batch      " << std::endl;
    std::cout << "===========================" << std::endl;
    output_batch.to_cpu(context, output_schema);
    output_batch.as_cpu()->print();
    std::cout << "===========================" << std::endl;
    std::cout << " The expected output batch " << std::endl;
    std::cout << "===========================" << std::endl;
    expected->print();

    EXPECT_TRUE(*output_batch.as_cpu() == *expected);
}
}  // namespace test
