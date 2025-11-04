#include <iostream>
#include <mmappet/mmappet.h>


int main()
{

    // Define a schema, it can be used to open multiple datasets with same schema
    Schema<uint32_t, uint32_t, uint32_t, uint32_t, uint32_t> schema("ClusterID", "frame", "scan", "tof", "intensity");

    // Create a new dataset writer
    auto writer = schema.create_writer("./test_write.mmappet");
    // Write some rows
    for(uint32_t i = 0; i < 10; ++i)
    {
        writer.write_row(i, i*10, i*100, i*1000, i*10000);
    }
    return 0;

}
