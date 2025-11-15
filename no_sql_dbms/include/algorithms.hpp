#pragma once
#include "vector.hpp"

template<typename T>
void custom_swap(T& a, T& b) {
    T temp = a;
    a = b;
    b = temp;
}

template<typename Iterator, typename Predicate>
Iterator custom_find_if(Iterator first, Iterator last, Predicate pred) {
    for (Iterator it = first; it != last; ++it) {
        if (pred(*it)) {
            return it;
        }
    }
    return last;
}

template<typename Iterator, typename T>
Iterator custom_find(Iterator first, Iterator last, const T& value) {
    for (Iterator it = first; it != last; ++it) {
        if (*it == value) {
            return it;
        }
    }
    return last;
}

template<typename Iterator, typename Predicate>
size_t custom_remove_if(Iterator first, Iterator last, Predicate pred) {
    Iterator result = first;
    size_t count = 0;
    while (first != last) {
        if (!pred(*first)) {
            *result = *first;
            ++result;
        } else {
            ++count;
        }
        ++first;
    }
    return count;
}

template<typename T>
void custom_sort(Vector<T>& vec) {
    for (size_t i = 0; i < vec.size(); ++i) {
        for (size_t j = 0; j < vec.size() - i - 1; ++j) {
            if (vec[j] > vec[j + 1]) {
                custom_swap(vec[j], vec[j + 1]);
            }
        }
    }
}
