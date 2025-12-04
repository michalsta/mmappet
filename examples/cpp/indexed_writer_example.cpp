#include <iostream>
#include <mmappet/mmappet.h>

int main()
{

    // Define a schema for the indexed dataset
    Schema<size_t, uint32_t, double> schema("SomeInt", "SmallerInt", "SomeFloat");

    // Create a new indexed dataset writer
    auto indexed_writer = schema.create_indexed_writer("./test_indexed.mmappet");

    // Write some groups
    for(size_t group_idx = 0; group_idx < 5; ++group_idx)
    {
        // Prepare data for the group
        size_t group_size = 3+group_idx; // Varying group size
        std::vector<size_t> some_ints(group_size);
        std::vector<uint32_t> smaller_ints(group_size);
        std::vector<double> some_floats(group_size);
        for(size_t i = 0; i < group_size; ++i)
        {
            some_ints[i] = group_idx * 10 + i;
            smaller_ints[i] = static_cast<uint32_t>(group_idx * 100 + i);
            some_floats[i] = group_idx * 0.5 + i * 0.1;
        }

        // Write the group
        indexed_writer.write_group(some_ints, smaller_ints, some_floats);
    }
}