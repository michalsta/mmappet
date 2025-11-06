#include <iostream>
#include <mmappet/mmappet.h>

int main()
{

    // Define a schema, it can be used to open multiple datasets with same schema
    Schema<size_t, uint32_t, double> schema("Index", "SomeInt", "SomeFloat");
    // Open a dataset directly
    auto dataset = schema.open_dataset("./test.mmappet");

    // Print dataset, row by row
    for(auto row : dataset)
    {
        auto [idx, some_int, some_float] = row;
        std::cout << idx << "\t" << some_int << "\t" << some_float << "\n";
    }

    // Or get individual columns. The same schema can be reused to open multiple datasets (or open same dataset multiple times)
    auto [C1, C2, C3] = schema.get_columns("./test.mmappet");
    // Print dataset again, using columnar access
    for(size_t i = 0; i < C1.size(); ++i)
    {
        std::cout << C1[i] << "\t" << C2[i] << "\t" << C3[i] << "\n";
    }

    // To open multiple datasets with same schema:
    // auto [D1_C1, D1_C2, D1_C3] = schema.get_columns("/path/somewhere/test1.mmappet");
    // auto [D2_C1, D2_C2, D2_C3] = schema.get_columns("/path/somewhere/test2.mmappet");

    // ... use D1_C*, D2_C* ...
}