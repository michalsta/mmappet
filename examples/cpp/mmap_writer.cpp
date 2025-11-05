#include <mmappet/mmappet.h>



int main()
{

    // Define a schema, it can be used to open multiple datasets with same schema
    Schema<size_t, uint32_t, double> schema("Index", "SomeInt", "SomeFloat");

    // The recommended way to create a dataset is to use DatasetWriter, see writer_example.cpp
    // However, one can also open a dataset in read-write mode and write to it directly, or modify existing data.
    // This might be advantageous if you have multiple threads/processes writing to the same dataset,
    // each one responsible for a different subset of rows. The way to do that is to resize the dataset
    // to the desired number of rows, then each thread/process can write to its assigned rows.
    // If the number of rows is not known in advance, one can first open the dataset in read-write mode,
    // resize it to a large number of rows, then after all writing is done, resize it back to the actual number of rows used.
    // Or, as new data appears, keep expanding it dynamic-vector-style (double the size when full), then at the end resize to actual size.
    // Note that resizing invalidates everything, so all threads/processes must coordinate to avoid accessing the dataset while another is resizing it.

    // First, an empty dataset must be created using DatasetWriter:
    {
        auto writer = schema.create_writer("./test_mmapped.mmappet");
        // Immediately close the writer as it goes out of scope, we just want to create an empty dataset
    }

    // Re-pen a dataset creating a read-write mapping
    auto dataset = schema.open_dataset("./test_mmapped.mmappet", false);
    dataset.resize(16); // Resize to 16 rows. These will be zero-filled initially. If the backing filesystem supports sparse files, this will not allocate disk space yet.

    // Write some data
    for(size_t i = 0; i < 1000; i++)
    {
        if(i >= dataset.size())
            dataset.resize(dataset.size() * 2); // Double size when full
        dataset.get_column<0>()[i] = i;
        dataset.get_column<1>()[i] = i * 10;
        dataset.get_column<2>()[i] = i * 0.1;
    }
    dataset.resize(1000); // Resize to actual size used
}