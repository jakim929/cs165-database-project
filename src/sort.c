#include "cs165_api.h"
#include "utils.h"


void mergesort(int size, int* arr, int* posn_vec) {
    int midpoint = (size) / 2;
    if (size == 1) {
        return;
    }
    if (size >= 2) {
        mergesort(midpoint, arr, posn_vec);
        mergesort(size - midpoint, arr + midpoint, posn_vec + midpoint);
    }

    int* left = arr;
    int* right = arr + midpoint;
    int* posn_vec_left = posn_vec;
    int* posn_vec_right = posn_vec + midpoint;
    int* midpoint_pointer = arr + midpoint;
    int* end = arr + size;
    
    int result[size];
    int posn_vec_result[size];
    for (int k = 0; k < size; k++) {
        if (right != end && (left == midpoint_pointer || left[0] > right[0])) {
            result[k] = right[0];
            posn_vec_result[k] = posn_vec_right[0];
            right++;
            posn_vec_right++;
        } else {
            result[k] = left[0];
            posn_vec_result[k] = posn_vec_left[0];
            left++;
            posn_vec_left++;
        }
    }

    for (int k = 0; k < size; k++) {
        arr[k] = result[k];
        posn_vec[k] = posn_vec_result[k];
    }
    
    return;
}
