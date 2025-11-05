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


template<typename T, typename U>
constexpr bool is_compatible_type()
{
    using T_plain = std::decay_t<T>;
    using U_plain = std::decay_t<U>;
    return sizeof(T_plain) == sizeof(U_plain) && (
           (std::is_integral<T_plain>::value && std::is_integral<U_plain>::value &&
            (std::is_signed<T_plain>::value == std::is_signed<U_plain>::value)) ||
           (std::is_floating_point<T_plain>::value && std::is_floating_point<U_plain>::value)
           );
}

template<typename T>
#if defined(__cpp_lib_constexpr_string) && __cpp_lib_constexpr_string >= 201907L
constexpr
#endif
std::string get_type_str()
{
    if constexpr (is_compatible_type<T, uint8_t>()) return "uint8";
    else if constexpr (is_compatible_type<T, int8_t>()) return "int8";
    else if constexpr (is_compatible_type<T, uint16_t>()) return "uint16";
    else if constexpr (is_compatible_type<T, int16_t>()) return "int16";
    else if constexpr (is_compatible_type<T, uint32_t>()) return "uint32";
    else if constexpr (is_compatible_type<T, int32_t>()) return "int32";
    else if constexpr (is_compatible_type<T, uint64_t>()) return "uint64";
    else if constexpr (is_compatible_type<T, int64_t>()) return "int64";
    else if constexpr (is_compatible_type<T, float>()) return "float32";
    else if constexpr (is_compatible_type<T, double>()) return "float64";
    else static_assert(false, "Unsupported type");
}

template<typename T>
class MMappedData {
    T* mappedData = nullptr;
    int fileDescriptor = -1;
    size_t dataSize = 0;
    size_t no_elements = 0;
    const std::filesystem::path filepath;
    int open_flags;
    int mmap_prot;
    int mmap_flags;

public:
    MMappedData(const std::filesystem::path& filepath, int open_flags = O_RDONLY, int mmap_prot = PROT_READ, int mmap_flags = MAP_SHARED) :
        filepath(filepath),
        open_flags(open_flags),
        mmap_prot(mmap_prot),
        mmap_flags(mmap_flags)
    {
        open_and_map(open_flags, mmap_prot, mmap_flags);
    }

    void open_and_map(int open_flags, int mmap_prot, int mmap_flags)
    {
        dataSize = std::filesystem::file_size(filepath);
        if(dataSize % sizeof(T) != 0)
            throw std::runtime_error("File size is not a multiple of element size for file: " + filepath.string());
        no_elements = dataSize / sizeof(T);

        fileDescriptor = open(filepath.c_str(), open_flags);
        if (fileDescriptor == -1)
            throw std::runtime_error("Failed to open file: " + filepath.string() + ", error: " + std::strerror(errno));

        if (no_elements == 0) {
            // Empty dataset, avoid mmap call, which would fail
            mappedData = nullptr;
            return;
        }

        mappedData = static_cast<T*>(mmap(nullptr, dataSize, mmap_prot, mmap_flags, fileDescriptor, 0));
        if (mappedData == MAP_FAILED)
        {
            close(fileDescriptor);
            fileDescriptor = -1;
            throw std::runtime_error("Failed to mmap file: " + filepath.string() + ", error: " + std::strerror(errno));
        }
    }
    void close_and_unmap()
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

    void resize(size_t new_no_elements)
    {
        close_and_unmap();
        dataSize = new_no_elements * sizeof(T);
        std::filesystem::resize_file(filepath, dataSize);
        open_and_map(open_flags, mmap_prot, mmap_flags);
    }

    ~MMappedData()
    {
        close_and_unmap();
    }

    MMappedData(const MMappedData&) = delete;
    MMappedData& operator=(const MMappedData&) = delete;
    MMappedData(MMappedData&& other) noexcept :
        mappedData(other.mappedData),
        fileDescriptor(other.fileDescriptor),
        dataSize(other.dataSize),
        no_elements(other.no_elements)
    {
        other.mappedData = nullptr;
        other.fileDescriptor = -1;
        other.dataSize = 0;
        other.no_elements = 0;
    }
    MMappedData& operator=(MMappedData&& other) noexcept = delete;

    inline T& operator[](size_t index) const {
        return mappedData[index];
    }

    inline size_t size() const {
        return no_elements;
    }

    inline T* data() const {
        return mappedData;
    }
};



template<typename... Args>
class Dataset {
public:
    Dataset(const std::filesystem::path&, std::vector<std::pair<std::string,std::string>>, size_t, int, int, int)
    {
        // Base case: do nothing
    }

    std::tuple<> move_columns()
    {
        return std::tuple<>();
    }

    std::tuple<> operator[](size_t)
    {
        return std::tuple<>();
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

    Dataset(const std::filesystem::path& filepath,
            const std::vector<std::pair<std::string, std::string>>& type_strs,
            size_t col_nr,
            int open_flags = O_RDONLY,
            int mmap_prot = PROT_READ,
            int mmap_flags = MAP_SHARED
        ) :
        type_str(type_strs[col_nr].first),
        column_name(type_strs[col_nr].second),
        column_number(col_nr),
        data(filepath / (std::to_string(col_nr) + ".bin"), open_flags, mmap_prot, mmap_flags),
        next_dataset(filepath, type_strs, col_nr + 1, open_flags, mmap_prot, mmap_flags)
    {
        if(type_str != get_type_str<T>())
            throw std::runtime_error("Type mismatch for column " + std::to_string(column_number) +
                                     ": expected " + get_type_str<T>() +
                                     ", got " + type_str);
        if constexpr (sizeof...(Args) > 0)
            if(next_dataset.size() != data.size())
                throw std::runtime_error("Column size mismatch between column " + std::to_string(column_number) +
                                     " and column " + std::to_string(column_number + 1));
    }

    template <size_t colnr>
    auto& get_column()
    {
        if constexpr (colnr == 0) {
            return data;
        } else {
            return next_dataset.template get_column<colnr - 1>();
        }
    }

    inline size_t size() const
    {
        return data.size();
    }

    auto move_columns()
    {
        return std::tuple_cat(std::make_tuple(std::move(data)), next_dataset.move_columns());
    }

    std::tuple<T, Args...> operator[](size_t index)
    {
        return std::tuple_cat(std::make_tuple(data[index]), next_dataset[index]);
    }

    class Iterator {
        size_t index;
        Dataset<T, Args...>* dataset;
    public:
        Iterator(size_t idx, Dataset<T, Args...>* ds) : index(idx), dataset(ds) {};
        inline std::tuple<T, Args...> operator*() {
            return (*dataset)[index];
        }
        inline Iterator& operator++() {
            ++index;
            return *this;
        }
        inline bool operator!=(const Iterator& other) const {
            return index != other.index;
        }
    };

    Iterator begin() {
        return Iterator(0, this);
    }
    Iterator end() {
        return Iterator(data.size(), this);
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


template<typename T, typename... Args>
auto OpenDataset(const std::filesystem::path& filepath, std::initializer_list<std::string> column_names, int open_flags = O_RDONLY, int mmap_prot = PROT_READ, int mmap_flags = MAP_SHARED)
{
    std::ifstream file;
    file.open(filepath / "schema.txt", std::ios::in | std::ios::binary);
    if(!file.is_open())
        throw std::runtime_error("Failed to open schema file: " + (filepath / "schema.txt").string() + ", error: " + std::strerror(errno));

    std::vector<std::pair<std::string, std::string>> tmp_type_strs;
    std::string s;
    while(std::getline(file, s))
    {
        if(s.empty())
            continue;
        size_t pos = s.find(' ');
        if (pos == std::string::npos)
            tmp_type_strs.emplace_back(s, "");
        else
            tmp_type_strs.emplace_back(s.substr(0, pos), s.substr(pos + 1));
    }
    file.close();

    if(column_names.size() != tmp_type_strs.size())
        throw std::runtime_error("Number of column names provided as argument does not match number of columns in file.");

    size_t ii = 0;
    for(auto it = column_names.begin(); it != column_names.end(); ++it, ++ii)
    {
        if(*it != tmp_type_strs[ii].second)
            throw std::runtime_error("Column name mismatch at column " + std::to_string(ii) +
                                     ": expected '" + *it +
                                     "', got '" + tmp_type_strs[ii].second + "'");
    }
    return Dataset<T, Args...>(filepath, tmp_type_strs, 0, open_flags, mmap_prot, mmap_flags);
};



template<size_t idx, typename T, typename... Args>
std::string schema_string_impl(const std::vector<std::string>& column_names)
{
    std::string result = get_type_str<T>() + " " + column_names[idx] + "\n";
    if constexpr (sizeof...(Args) == 0)
    {
        return result;
    }
    else
    {
        result += schema_string_impl<idx + 1, Args...>(column_names);
        return result;
    }
}
template<typename... Args>
class DatasetWriter {
public:
    DatasetWriter(const std::filesystem::path&, size_t) {}
    void write_row() {}
    void write_rows(size_t) {}
};

template<typename T, typename... Args>
class DatasetWriter<T, Args...> {
    std::ofstream file;
    DatasetWriter<Args...> next_writer;

public:
    DatasetWriter(const std::filesystem::path& filepath, size_t col_nr) :
        file(),
        next_writer(filepath, col_nr + 1)
    {
        file.open(filepath / (std::to_string(col_nr) + ".bin"), std::ios::out | std::ios::binary | std::ios::trunc);
        if(!file.is_open())
            throw std::runtime_error("Failed to open file for writing: " + (filepath / (std::to_string(col_nr) + ".bin")).string() + ", error: " + std::strerror(errno));
    }

    void write_row(const T& value, const Args&... args)
    {
        file.write(reinterpret_cast<const char*>(&value), sizeof(T));
        next_writer.write_row(args...);
    }

    void write_rows(size_t n, const T* values, const Args*... args)
    {
        file.write(reinterpret_cast<const char*>(values), n * sizeof(T));
        next_writer.write_rows(n, args...);
    }
};


template<typename T, typename... Args>
class Schema
{
    std::vector<std::string> column_names;

    template<size_t... Is>
    auto open_dataset_impl(const std::filesystem::path& filepath, bool readonly, std::index_sequence<Is...>)
    {
        int open_flags = readonly ? O_RDONLY : O_RDWR;
        int mmap_prot = readonly ? PROT_READ : (PROT_READ | PROT_WRITE);
        int mmap_flags = MAP_SHARED;
        return OpenDataset<T, Args...>(filepath, {column_names[Is]...}, open_flags, mmap_prot, mmap_flags);
    }

    template<size_t idx, typename U, typename... Rest>
    std::string schema_string_impl() const
    {
        std::string result = get_type_str<U>() + " " + column_names[idx] + "\n";
        if constexpr (sizeof...(Rest) == 0)
        {
            return result;
        }
        else
        {
            result += schema_string_impl<idx + 1, Rest...>();
            return result;
        }
    }


    public:
    template<typename... Strings>
    Schema(const Strings&... col_names)
    { (column_names.push_back(col_names), ...);};

    auto open_dataset(const std::filesystem::path& filepath, bool readonly = true)
    {
        return open_dataset_impl(filepath, readonly, std::make_index_sequence<sizeof...(Args)+1>{});
    }

    auto get_columns(const std::filesystem::path& filepath, bool readonly = true)
    {
        auto dataset = open_dataset(filepath, readonly);
        return dataset.move_columns();
    }

    std::string schema_string() const
    {
        return schema_string_impl<0, T, Args...>();
    }

    void write_schema_file(const std::filesystem::path& filepath) const
    {
        std::ofstream file;
        file.open(filepath, std::ios::out |  std::ios::trunc | std::ios::binary);
        if(!file.is_open())
            throw std::runtime_error("Failed to open schema file for writing: " + filepath.string() + ", error: " + std::strerror(errno));
        file << schema_string();
        file.close();
    }

    DatasetWriter<T, Args...> create_writer(const std::filesystem::path& filepath)
    {
        std::filesystem::create_directories(filepath);
        write_schema_file(filepath / "schema.txt");
        return DatasetWriter<T, Args...>(filepath, 0);
    }

};
