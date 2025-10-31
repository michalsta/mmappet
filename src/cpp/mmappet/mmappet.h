#include <string>
#include <fstream>
#include <filesystem>
#include <vector>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdexcept>
#include <cstring>
#include <iostream>


template<typename T>
constexpr std::string get_type_str()
{
    if constexpr (std::is_same<T, uint8_t>::value) return "uint8";
    else if constexpr (std::is_same<T, int8_t>::value) return "int8";
    else if constexpr (std::is_same<T, uint16_t>::value) return "uint16";
    else if constexpr (std::is_same<T, int16_t>::value) return "int16";
    else if constexpr (std::is_same<T, uint32_t>::value) return "uint32";
    else if constexpr (std::is_same<T, int32_t>::value) return "int32";
    else if constexpr (std::is_same<T, uint64_t>::value) return "uint64";
    else if constexpr (std::is_same<T, int64_t>::value) return "int64";
    else if constexpr (std::is_same<T, float>::value) return "float32";
    else if constexpr (std::is_same<T, double>::value) return "float64";
    else static_assert("Unsupported type");
}

template<typename T>
class MMappedData {
    T* mappedData = nullptr;
    int fileDescriptor = -1;
    size_t dataSize = 0;
    size_t no_elements = 0;

public:
    MMappedData(const std::filesystem::path& filepath, int open_flags = O_RDONLY, int mmap_prot = PROT_READ, int mmap_flags = MAP_SHARED)
    {
        dataSize = std::filesystem::file_size(filepath);
        if(dataSize % sizeof(T) != 0)
            throw std::runtime_error("File size is not a multiple of element size for file: " + filepath.string());
        no_elements = dataSize / sizeof(T);

        fileDescriptor = open(filepath.c_str(), open_flags);
        if (fileDescriptor == -1)
            throw std::runtime_error("Failed to open file: " + filepath.string() + ", error: " + std::strerror(errno));

        mappedData = static_cast<T*>(mmap(nullptr, dataSize, mmap_prot, mmap_flags, fileDescriptor, 0));
        if (mappedData == MAP_FAILED)
        {
            close(fileDescriptor);
            fileDescriptor = -1;
            throw std::runtime_error("Failed to mmap file: " + filepath.string() + ", error: " + std::strerror(errno));
        }
    }
    ~MMappedData()
    {
        if (mappedData)
        {
            munmap(mappedData, dataSize);
        }
        if (fileDescriptor != -1)
        {
            close(fileDescriptor);
        }
    }

    MMappedData(const MMappedData&) = delete;
    MMappedData& operator=(const MMappedData&) = delete;
    MMappedData(MMappedData&& other) noexcept :
        mappedData(other.mappedData),
        fileDescriptor(other.fileDescriptor),
        dataSize(other.dataSize)
    {
        other.mappedData = nullptr;
        other.fileDescriptor = -1;
        other.dataSize = 0;
    }
    MMappedData& operator=(MMappedData&& other) noexcept = delete;

    inline T& operator[](size_t index) const {
        return mappedData[index];
    }

    inline size_t size() const {
        return no_elements;
    }
};


std::pair<std::string, std::string>
split_first_space(const std::string& s) {
    size_t pos = s.find(' ');
    if (pos == std::string::npos) {
        return {s, ""};
    }
    return { s.substr(0, pos), s.substr(pos + 1) };
}





template<typename... Args>
class Dataset {
public:
    Dataset(const std::filesystem::path& filepath, std::vector<std::string> type_strs, size_t col_nr)
    {
        // Base case: do nothing
    }
};

template<typename T, typename... Args>
class Dataset<T, Args...>
{
    const std::string type_str;
    const std::string column_name;
    const size_t column_number;
    MMappedData<T> data;
    Dataset<Args...> next_dataset;

public:

    Dataset(const std::filesystem::path& filepath, const std::vector<std::string>& type_strs, size_t col_nr) :
        type_str(split_first_space(type_strs.back()).first),
        column_name(split_first_space(type_strs.back()).second),
        column_number(col_nr),
        data(filepath / (std::to_string(col_nr) + ".bin")),
        next_dataset(filepath, type_strs, col_nr + 1)
    {
        if(type_str != get_type_str<T>())
            throw std::runtime_error("Type mismatch for column " + std::to_string(column_number) +
                                     ": expected " + get_type_str<T>() +
                                     ", got " + type_str);
    }

    template <size_t colnr, typename U>
    inline MMappedData<U>& get_column()
    {
        static_assert(colnr < (1 + sizeof...(Args)), "Column index out of range");
        if constexpr (colnr == 0)
        {
            static_assert(std::is_same<U, T>::value, "Type mismatch for requested column");
            return data;
        }
        else
        {
            return next_dataset.template get_column<colnr - 1, U>();
        }
    }

};

template<typename T, typename... Args>
auto OpenDataset(const std::filesystem::path& filepath)
{
    std::ifstream file;
    file.exceptions(std::ifstream::failbit | std::ifstream::badbit);
    file.open(filepath / "schema.txt", std::ios::in | std::ios::binary);
    file.exceptions(std::ifstream::goodbit);

    std::vector<std::string> tmp_type_strs;
    std::string tmp_type_str;
    while(std::getline(file, tmp_type_str))
    {
        std::cerr << "Read type string: " << tmp_type_str << std::endl;
        tmp_type_strs.push_back(tmp_type_str);
    }
    file.close();
    return Dataset<T, Args...>(filepath, tmp_type_strs, 0);
}



template<typename... Args>
class DatasetWriter {
};

template<typename T, typename... Args>
class DatasetWriter<T, Args...> {
    const std::string type_str;
    const std::string column_name;
    const size_t column_number;
    std::ofstream file;
    DatasetWriter<Args...> next_dataset_writer;
public:
    DatasetWriter(const std::filesystem::path& filepath,
                  std::ofstream& schema_file,
                  size_t col_nr,
                  const std::vector<std::string>& column_names
                  bool append = false) :
        type_str(get_type_str<T>()),
        column_name(column_names[col_nr]),
        column_number(col_nr),
        file(),
        next_dataset_writer(filepath, schema_file, col_nr + 1, column_names)
    {
        if(!append)
            schema_file << type_str << " " << column_name << "\n";
        file.exceptions(std::ofstream::failbit | std::ofstream::badbit);
        file.open(filepath / (std::to_string(col_nr) + ".bin"), std::ios::out | std::ios::binary | std::ios::trunc);

    }