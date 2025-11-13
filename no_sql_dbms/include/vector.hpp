#pragma once
#include <cstdlib>
#include <stdexcept>
#include <utility>

template<typename T>
class Vector {
public:
    Vector() : data_(nullptr), size_(0), capacity_(0) {}

    explicit Vector(size_t count) : data_(nullptr), size_(0), capacity_(0) {
        resize(count);
    }

    Vector(size_t count, const T& value) : data_(nullptr), size_(0), capacity_(0) {
        resize(count);
        for (size_t i = 0; i < count; ++i) {
            data_[i] = value;
        }
    }

    ~Vector() {
        clear();
        free(data_);
    }

    Vector(const Vector& other) : data_(nullptr), size_(0), capacity_(0) {
        reserve(other.size_);
        for (size_t i = 0; i < other.size_; ++i) {
            new(data_ + i) T(other.data_[i]);
        }
        size_ = other.size_;
    }

    Vector& operator=(const Vector& other) {
        if (this != &other) {
            clear();
            reserve(other.size_);
            for (size_t i = 0; i < other.size_; ++i) {
                new(data_ + i) T(other.data_[i]);
            }
            size_ = other.size_;
        }
        return *this;
    }

    void push_back(const T& value) {
        if (size_ >= capacity_) {
            reserve(capacity_ == 0 ? 4 : capacity_ * 2);
        }
        new(data_ + size_) T(value);
        ++size_;
    }

    void push_back(T&& value) {
        if (size_ >= capacity_) {
            reserve(capacity_ == 0 ? 4 : capacity_ * 2);
        }
        new(data_ + size_) T(std::move(value));
        ++size_;
    }

    template<typename... Args>
    void emplace_back(Args&&... args) {
        if (size_ >= capacity_) {
            reserve(capacity_ == 0 ? 4 : capacity_ * 2);
        }
        new(data_ + size_) T(std::forward<Args>(args)...);
        ++size_;
    }

    void pop_back() {
        if (size_ > 0) {
            data_[size_ - 1].~T();
            --size_;
        }
    }

    T& operator[](size_t index) {
        return data_[index];
    }

    const T& operator[](size_t index) const {
        return data_[index];
    }

    T& at(size_t index) {
        if (index >= size_) throw std::out_of_range("Vector index out of range");
        return data_[index];
    }

    const T& at(size_t index) const {
        if (index >= size_) throw std::out_of_range("Vector index out of range");
        return data_[index];
    }

    T& front() { return data_[0]; }
    const T& front() const { return data_[0]; }

    T& back() { return data_[size_ - 1]; }
    const T& back() const { return data_[size_ - 1]; }

    T* data() { return data_; }
    const T* data() const { return data_; }

    bool empty() const { return size_ == 0; }
    size_t size() const { return size_; }
    size_t capacity() const { return capacity_; }

    void clear() {
        for (size_t i = 0; i < size_; ++i) {
            data_[i].~T();
        }
        size_ = 0;
    }

    void resize(size_t new_size) {
        if (new_size > capacity_) {
            reserve(new_size);
        }
        if (new_size > size_) {
            for (size_t i = size_; i < new_size; ++i) {
                new(data_ + i) T();
            }
        } else {
            for (size_t i = new_size; i < size_; ++i) {
                data_[i].~T();
            }
        }
        size_ = new_size;
    }

    void reserve(size_t new_capacity) {
        if (new_capacity <= capacity_) return;

        T* new_data = static_cast<T*>(malloc(new_capacity * sizeof(T)));
        if (!new_data) throw std::bad_alloc();

        for (size_t i = 0; i < size_; ++i) {
            new(new_data + i) T(std::move(data_[i]));
            data_[i].~T();
        }

        free(data_);
        data_ = new_data;
        capacity_ = new_capacity;
    }

    void erase(size_t pos) {
        if (pos >= size_) return;
        data_[pos].~T();
        for (size_t i = pos; i < size_ - 1; ++i) {
            new(data_ + i) T(std::move(data_[i + 1]));
            data_[i + 1].~T();
        }
        --size_;
    }

    void insert(size_t pos, const T& value) {
        if (pos > size_) return;
        if (size_ >= capacity_) {
            reserve(capacity_ == 0 ? 4 : capacity_ * 2);
        }
        for (size_t i = size_; i > pos; --i) {
            new(data_ + i) T(std::move(data_[i - 1]));
            data_[i - 1].~T();
        }
        new(data_ + pos) T(value);
        ++size_;
    }

    using iterator = T*;
    using const_iterator = const T*;

    iterator begin() { return data_; }
    iterator end() { return data_ + size_; }
    const_iterator begin() const { return data_; }
    const_iterator end() const { return data_ + size_; }
    const_iterator cbegin() const { return data_; }
    const_iterator cend() const { return data_ + size_; }

private:
    T* data_;
    size_t size_;
    size_t capacity_;
};
