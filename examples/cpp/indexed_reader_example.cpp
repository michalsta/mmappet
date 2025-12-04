#include <iostream>
#include <mmappet/mmappet.h>

int main()
{

    // Define a schema for the indexed dataset
    Schema<size_t, uint32_t, double> schema("SomeInt", "SmallerInt", "SomeFloat");

    // Open an indexed dataset
    auto indexed_dataset = schema.open_indexed_dataset("./test_indexed.mmappet");

    // Read and print groups
    for(size_t group_idx = 0; group_idx < indexed_dataset.number_of_groups(); ++group_idx)
    {
        auto [some_ints, smaller_ints, some_floats] = indexed_dataset.get_group(group_idx);
        std::cout << "Group " << group_idx << ":\n";
        for(size_t i = 0; i < some_ints.size(); ++i)
        {
            std::cout << "\t" << some_ints[i] << "\t" << smaller_ints[i] << "\t" << some_floats[i] << "\n";
        }
    }
}