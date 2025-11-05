#include <iostream>
#include <mmappet/mmappet.h>


int main()
{

    // Define a schema, it can be used to open multiple datasets with same schema
    Schema<size_t, uint32_t, double> schema("Index", "SomeInt", "SomeFloat");

    // Create a new dataset writer
    auto writer = schema.create_writer("./test.mmappet");
    // Write some rows
    for(size_t i = 0; i < 10; ++i)
    {
        writer.write_row(i, i*10, i*0.1);
    }
    return 0;

}
