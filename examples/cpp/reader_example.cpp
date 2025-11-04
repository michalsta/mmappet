#include <mmappet/mmappet.h>


int main()
{

    // Define a schema, it can be used to open multiple datasets with same schema
    Schema<uint32_t, uint32_t, uint32_t, uint32_t, uint32_t> schema("ClusterID", "frame", "scan", "tof", "intensity");

    // Open a dataset directly
    auto dataset = schema.open_dataset("/Users/mist/git/massimo_cpp/test1.mmappet");

    // Print dataset, row by row
    for(auto row : dataset)
    {
        auto [cluster_id, frame, scan, tof, intensity] = row;
        std::cout << cluster_id << "\t" << frame << "\t" << scan << "\t" << tof << "\t" << intensity << "\n";
    }

    // Or get individual columns
    auto [C1, C2, C3, C4, C5] = schema.get_columns("/Users/mist/git/massimo_cpp/test1.mmappet");
    // Print dataset again, using columnar access
    for(size_t i = 0; i < C1.size(); ++i)
    {
        std::cout << C1[i] << "\t" << C2[i] << "\t" << C3[i] << "\t" << C4[i] << "\t" << C5[i] << "\n";
    }

    // To open multiple datasets with same schema:
    auto [D1_C1, D1_C2, D1_C3, D1_C4, D1_C5] = schema.get_columns("/Users/mist/git/massimo_cpp/test1.mmappet");
    auto [D2_C1, D2_C2, D2_C3, D2_C4, D2_C5] = schema.get_columns("/Users/mist/git/massimo_cpp/test2.mmappet");

    // ... use D1_C*, D2_C* ...
}