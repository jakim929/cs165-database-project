#include <smmintrin.h>
#include <immintrin.h>

long simd_sum(int* arr, int size) {
    int res[8] = { 0, 0 , 0, 0, 0, 0, 0, 0 };
    __m256i res_vec = _mm256_loadu_si256((__m256i*) res);

    int i = 0;
    for (; i < size - 8; i+=8) {
        __m256i arr_values = _mm256_loadu_si256((__m256i*) &arr[i]);
        res_vec = _mm256_add_epi32(res_vec, arr_values);
    }

    _mm256_storeu_si256((__m256i*) res, res_vec);

    for (; i < size; i++) {
        res[i % 8] += arr[i];
    }

    long sum = 0;
    for (int k = 0; k < 8; k++) {
        sum += (long) res[k];
    }
    
    return sum;
}

void simd_add(int* res, int* arr1, int* arr2, int size) {
    int i = 0;
    for (; i < size - 8; i+=8) {
        __m256i arr1_vec = _mm256_loadu_si256((__m256i*) &arr1[i]);
        __m256i arr2_vec = _mm256_loadu_si256((__m256i*) &arr2[i]);
        arr1_vec = _mm256_add_epi32(arr1_vec, arr2_vec);
        _mm256_storeu_si256((__m256i*) &res[i], arr1_vec);
    }

    for (; i < size; i++) {
        res[i] = arr1[i] + arr2[i];
    }
}