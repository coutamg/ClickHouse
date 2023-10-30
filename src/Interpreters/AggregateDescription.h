#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/ColumnNumbers.h>
#include <Core/Names.h>
#include <Core/Types.h>

namespace DB
{

namespace JSONBuilder { class JSONMap; }

// select sum(a) from t1;
struct AggregateDescription
{
    AggregateFunctionPtr function;
    
    Array parameters;        /// Parameters of the (parametric) aggregate function.
    // argument_names = {a}
    Names argument_names;
    // column_name 为 sum(a)
    String column_name;      /// What name to use for a column with aggregate function values

    void explain(WriteBuffer & out, size_t indent) const; /// Get description for EXPLAIN query.
    void explain(JSONBuilder::JSONMap & map) const;
};

using AggregateDescriptions = std::vector<AggregateDescription>;
}
