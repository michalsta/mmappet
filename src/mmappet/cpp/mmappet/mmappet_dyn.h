#pragma once

#include <cstdint>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>
#include <tuple>
#include <span>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <cstring>
#include <cerrno>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

#include "mmappet.h"

inline size_t element_size_from_type_str(std::string_view ts) {
    if (ts == "uint8"  || ts == "int8")   return 1;
    if (ts == "uint16" || ts == "int16")  return 2;
    if (ts == "uint32" || ts == "int32" || ts == "float32") return 4;
    if (ts == "uint64" || ts == "int64" || ts == "float64") return 8;
    throw std::runtime_error("Unknown type string: " + std::string(ts));
}

class Column {
    void* data_ = nullptr;
    int fd_ = -1;
    size_t mapped_size_ = 0;
    size_t elem_size_ = 0;
    size_t count_ = 0;
    std::string type_str_;
    std::string name_;

public:
    Column(const std::filesystem::path& bin_path,
           std::string type_str,
           std::string name)
        : type_str_(std::move(type_str)), name_(std::move(name))
    {
        elem_size_ = element_size_from_type_str(type_str_);
        mapped_size_ = std::filesystem::file_size(bin_path);
        if (mapped_size_ % elem_size_ != 0)
            throw std::runtime_error("File size not a multiple of element size: " + bin_path.string());
        count_ = mapped_size_ / elem_size_;

        fd_ = open(bin_path.c_str(), O_RDONLY);
        if (fd_ == -1)
            throw std::runtime_error("Failed to open file: " + bin_path.string() + ": " + std::strerror(errno));

        if (count_ == 0) {
            data_ = nullptr;
            return;
        }

        void* raw = mmap(nullptr, mapped_size_, PROT_READ, MAP_SHARED, fd_, 0);
        if (raw == MAP_FAILED) {
            ::close(fd_);
            fd_ = -1;
            throw std::runtime_error("Failed to mmap file: " + bin_path.string() + ": " + std::strerror(errno));
        }
        data_ = raw;
    }

    ~Column() noexcept {
        if (data_) munmap(data_, mapped_size_);
        if (fd_ != -1) ::close(fd_);
    }

    Column(const Column&) = delete;
    Column& operator=(const Column&) = delete;

    Column(Column&& o) noexcept
        : data_(o.data_), fd_(o.fd_), mapped_size_(o.mapped_size_),
          elem_size_(o.elem_size_), count_(o.count_),
          type_str_(std::move(o.type_str_)), name_(std::move(o.name_))
    {
        o.data_ = nullptr;
        o.fd_ = -1;
        o.mapped_size_ = 0;
        o.count_ = 0;
    }

    Column& operator=(Column&& o) noexcept {
        if (this != &o) {
            if (data_) munmap(data_, mapped_size_);
            if (fd_ != -1) ::close(fd_);
            data_ = o.data_;
            fd_ = o.fd_;
            mapped_size_ = o.mapped_size_;
            elem_size_ = o.elem_size_;
            count_ = o.count_;
            type_str_ = std::move(o.type_str_);
            name_ = std::move(o.name_);
            o.data_ = nullptr;
            o.fd_ = -1;
            o.mapped_size_ = 0;
            o.count_ = 0;
        }
        return *this;
    }

    const std::string& name() const noexcept { return name_; }
    const std::string& type_str() const noexcept { return type_str_; }
    size_t size() const noexcept { return count_; }
    size_t element_size() const noexcept { return elem_size_; }
    const std::byte* raw_data() const noexcept { return static_cast<const std::byte*>(data_); }

    template<typename T>
    std::span<const T> as() const {
        if (sizeof(T) != elem_size_)
            throw std::runtime_error("Type size mismatch in Column::as<T>(): sizeof(T)=" +
                std::to_string(sizeof(T)) + " but element_size=" + std::to_string(elem_size_));
        if (get_type_str<T>() != type_str_)
            throw std::runtime_error("Type mismatch in Column::as<T>(): requested " +
                get_type_str<T>() + " but column is " + type_str_);
        return std::span<const T>(static_cast<const T*>(data_), count_);
    }
};

class DynDataset {
protected:
    std::vector<Column> columns_;

public:
    DynDataset() = default;
    explicit DynDataset(std::vector<Column> cols) : columns_(std::move(cols)) {}

    DynDataset(const DynDataset&) = delete;
    DynDataset& operator=(const DynDataset&) = delete;
    DynDataset(DynDataset&&) = default;
    DynDataset& operator=(DynDataset&&) = default;
    virtual ~DynDataset() = default;

    Column& operator[](size_t idx) { return columns_[idx]; }
    const Column& operator[](size_t idx) const { return columns_[idx]; }

    Column& operator[](std::string_view name) {
        for (auto& c : columns_)
            if (c.name() == name) return c;
        throw std::runtime_error("Column not found: " + std::string(name));
    }
    const Column& operator[](std::string_view name) const {
        for (auto& c : columns_)
            if (c.name() == name) return c;
        throw std::runtime_error("Column not found: " + std::string(name));
    }

    size_t num_columns() const noexcept { return columns_.size(); }
    size_t num_rows() const noexcept { return columns_.empty() ? 0 : columns_[0].size(); }

    auto begin() { return columns_.begin(); }
    auto end() { return columns_.end(); }
    auto begin() const { return columns_.begin(); }
    auto end() const { return columns_.end(); }
};

template<typename T, typename... Args>
class TypedDataset : public DynDataset {
public:
    using DynDataset::DynDataset;

    static TypedDataset open(const std::filesystem::path& path,
                             std::initializer_list<std::string> col_names) {
        std::ifstream file(path / "schema.txt");
        if (!file.is_open())
            throw std::runtime_error("Failed to open schema: " + (path / "schema.txt").string());

        std::vector<std::pair<std::string, std::string>> schema;
        std::string line;
        while (std::getline(file, line)) {
            if (line.empty()) continue;
            auto [type, name] = split_first_space(line);
            schema.emplace_back(type, name);
        }

        constexpr size_t expected_cols = 1 + sizeof...(Args);
        if (schema.size() != expected_cols)
            throw std::runtime_error("Column count mismatch: schema has " +
                std::to_string(schema.size()) + " but template expects " +
                std::to_string(expected_cols));

        if (col_names.size() != expected_cols)
            throw std::runtime_error("Column name count mismatch");

        // Validate types match template args
        validate_types<0, T, Args...>(schema);

        // Validate names match
        size_t i = 0;
        for (auto it = col_names.begin(); it != col_names.end(); ++it, ++i) {
            if (*it != schema[i].second)
                throw std::runtime_error("Column name mismatch at " + std::to_string(i) +
                    ": expected '" + *it + "', got '" + schema[i].second + "'");
        }

        // mmap each column
        std::vector<Column> cols;
        cols.reserve(expected_cols);
        for (size_t j = 0; j < expected_cols; ++j) {
            cols.emplace_back(path / (std::to_string(j) + ".bin"),
                              schema[j].first, schema[j].second);
        }

        return TypedDataset(std::move(cols));
    }

    auto typed_columns() const {
        return typed_columns_impl<0, T, Args...>();
    }

private:
    template<size_t idx, typename U, typename... Rest>
    static void validate_types(const std::vector<std::pair<std::string, std::string>>& schema) {
        if (schema[idx].first != get_type_str<U>())
            throw std::runtime_error("Type mismatch at column " + std::to_string(idx) +
                ": schema has " + schema[idx].first + " but template expects " + get_type_str<U>());
        if constexpr (sizeof...(Rest) > 0)
            validate_types<idx + 1, Rest...>(schema);
    }

    template<size_t idx, typename U, typename... Rest>
    auto typed_columns_impl() const {
        if constexpr (sizeof...(Rest) == 0) {
            return std::make_tuple(columns_[idx].template as<U>());
        } else {
            return std::tuple_cat(
                std::make_tuple(columns_[idx].template as<U>()),
                typed_columns_impl<idx + 1, Rest...>());
        }
    }
};
