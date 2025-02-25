#ifndef _WINDOW_BIT_COUNT_APX_
#define _WINDOW_BIT_COUNT_APX_

#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <math.h>
#include <string.h>

////////////////////////////////////////////////////////////////////////////////
// Global merge count for debugging purposes only
////////////////////////////////////////////////////////////////////////////////
uint64_t N_MERGES = 0;

////////////////////////////////////////////////////////////////////////////////
// Bucket structure:
//  oldest_time : Timestamp of the earliest 1 in the bucket
//  newest_time : Timestamp of the latest 1 in the bucket
//  size        : Total number of 1s in this bucket
////////////////////////////////////////////////////////////////////////////////
typedef struct {
    uint32_t oldest_time;
    uint32_t newest_time;
    uint32_t size;
} Bucket;

      //
typedef struct {       // StateApx: Holds the state of the sliding window counter
    uint32_t wnd_size;       // Window size W
    uint32_t k;        // Merge threshold (merge if consecutive buckets of the same size > k+1)
    uint32_t current_time;       //Timestamp of the processed bit (increments by 1 per bit received)
    uint32_t count;       //Total number of 1s represented by all buckets
    uint32_t bucket_count;      // Number of buckets actually used in buckets[]
    Bucket*  buckets;        //Bucket array (allocated once during initialization)
    uint32_t total_bucket;       //Capacity limit of the buckets[] array
} StateApx;


// Initialization:
//1. Allocate a buckets[] array of size (k+1)*(log(wnd_size / k)+1)
//2. Return the number of bytes allocated
uint64_t wnd_bit_count_apx_new(StateApx* self, uint32_t wnd_size, uint32_t k) {
    assert(wnd_size >= 1 && k >= 1);

    self->wnd_size = wnd_size;
    self->k = k;
    self->current_time = 0;
    self->count = 0;
    self->bucket_count = 0;

    // Buckets maximum  capacity
    if (self->k < self->wnd_size) {
    }
    self->total_bucket = (k + 1) * ((uint32_t)ceil(log2((double)wnd_size / k)) + 1);

    uint32_t mem = self->total_bucket * sizeof(Bucket);
    self->buckets = (Bucket*)malloc(mem);
    assert(self->buckets != NULL);

    return mem;
}


// Destructor: Free the buckets array
void wnd_bit_count_apx_destruct(StateApx* self) {
    free(self->buckets);
    self->buckets = NULL;
}


// (Optional) Debug print
void wnd_bit_count_apx_print(StateApx* self) {
//TO DO
}


// Merge buckets: Ensure no more than k+1 buckets of the same size exist
void merge_buckets(StateApx* self) {
    // If fewer than 3 buckets, no merging needed
    if (self->bucket_count < 3) return;


    // Count consecutive buckets of the same size and track oldest/next oldest
    uint32_t consecutive = 1;            // Current count of consecutive buckets of the same size
    uint32_t cur_size = 1;               // Current size being tracked
    int pos = self->bucket_count - 2;     // Position before the newest bucket

    // Use while(true) + break to ensure full merging
    while (pos >= 0) {
        Bucket* b = &self->buckets[pos];
        if (b->size == cur_size) {
            consecutive++;
            if (consecutive > (self->k + 1)) {
                // Merge b and b+1
                self->buckets[pos].size *= 2;

                // oldest_time takes the earlier timestamp
                Bucket* next_b = &self->buckets[pos + 1];
                if (next_b->oldest_time < b->oldest_time) {
                    b->oldest_time = next_b->oldest_time;
                }
                // newest_time takes the later timestamp
                if (next_b->newest_time > b->newest_time) {
                    b->newest_time = next_b->newest_time;
                }
                // Remove next_b
                uint32_t move_count = self->bucket_count - (pos + 2);
                if (move_count > 0) {
                    memmove(&self->buckets[pos + 1],
                            &self->buckets[pos + 2],
                            move_count * sizeof(Bucket));
                }
                self->bucket_count--;
                N_MERGES++;

                // After merging, size doubles, possibly requiring further merging
                cur_size = b->size;
                consecutive = 1;
                // Keep pos unchanged to recheck this position (previous buckets might be the same size)
            } else {
                pos--;
            }
        } else {
            // Size changed, reset
            cur_size = b->size;
            consecutive = 1;
            pos--;
        }
    }
    
}


// Update buckets: Handle new bit, remove expired buckets, and insert if needed
void update_buckets(StateApx* self, bool item) {


    // Use binary search to batch remove expired buckets
    //    cut_point = current_time - wnd_size, if newest_time <= cut_point, the bucket expires
    uint32_t cut_point = (self->current_time > self->wnd_size) ? (self->current_time - self->wnd_size) : 0;

    uint32_t left = 0, right = self->bucket_count;
    while (left < right) {
        uint32_t mid = left + (right - left) / 2;
        if (self->buckets[mid].newest_time <= cut_point) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    // left is the number of expired buckets
    if (left > 0) {
        // Calculate the total size of expired buckets
        uint32_t expired_sum = 0;
        for (uint32_t i = 0; i < left; i++) {
            expired_sum += self->buckets[i].size;
        }
        self->count -= expired_sum;

        // Move remaining buckets
        uint32_t remain = self->bucket_count - left;
        if (remain > 0) {
            memmove(self->buckets, &self->buckets[left],
                    remain * sizeof(Bucket));
        }
        self->bucket_count = remain;
    }

    // 3. If the bit is 1, insert a new bucket (size=1) and trigger a full merge
    if (item) {
        // Capacity check
        if (self->bucket_count >= self->total_bucket) {
            fprintf(stderr, "Bucket overflow! capacity=%u\n", self->total_bucket);
            exit(EXIT_FAILURE);
        }

        // Insert new bucket
        Bucket* new_b = &self->buckets[self->bucket_count];
        new_b->oldest_time = self->current_time;
        new_b->newest_time = self->current_time;
        new_b->size = 1;
        self->bucket_count++;
        self->count++;

        // Call merge function to handle bucket merging
        merge_buckets(self);
    }
}


// main function: Process the next bit in the stream
//  1) Increment current_time
//  2) Update buckest(including 1.remove the expired buckets; 2.add new bit into the buckets + merge operation;3.3) If item=1, insert a new bucket and trigger a full merge)
uint32_t wnd_bit_count_apx_next(StateApx* self, bool item) {

    // Increment time
    self->current_time++;
    // Update buckets with the new bit
    update_buckets(self, item);

    // Compute approximation: approx = count - oldest.size/2 
    uint32_t last_output_apx = self->count;
    if (self->bucket_count > 0) {
        uint32_t half = self->buckets[0].size / 2;

        last_output_apx -= half;
    }

    return last_output_apx;
}

#endif // _WINDOW_BIT_COUNT_APX_