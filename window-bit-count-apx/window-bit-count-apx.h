#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <math.h>

uint64_t N_MERGES = 0; // Track bucket merges

typedef struct {
    uint32_t size;      // Bucket size (number of 1’s)
    uint32_t timestamp; // Bucket timestamp
} Bucket;

typedef struct {
    uint32_t window_size;        // 窗口大小 W
    uint32_t k;        // Max same-size buckets
    uint32_t max_buckets; //Estimated maximum of number of  buckets;
    uint32_t current_buckets;    // 当前桶数量
    uint32_t current_time;   // 当前时间戳

    Bucket* buckets;    // Bucket array

    uint32_t total_ones;   // Total 1’s in window
    uint32_t oldest_valid_index; // Oldest bucket index
} StateApx;

uint64_t wnd_bit_count_apx_new(StateApx* self, uint32_t wnd_size, uint32_t k) {
    assert(wnd_size >= 1);
    assert(k >= 1);

    self->window_size = wnd_size;
    self->k = k;
    self->max_buckets = k * (int)(log2(self->window_size/k) + 1);
    self->current_buckets = 0;
    self->current_time = 0;
    uint64_t memory = self->max_buckets * sizeof(Bucket);
    self->buckets = (Bucket*)malloc(memory);
 
    self->total_ones = 0;
    self->oldest_valid_index = -1;
    if (!self->buckets) {
        fprintf(stderr, "Memory allocation failed!\n");
        exit(1);
    }
    for (int i = 0; i <  self->max_buckets; i++) {
        self->buckets[i].size = 0;
        self->buckets[i].timestamp = -1; // 表示未使用
    }


    return memory;
}

void wnd_bit_count_apx_destruct(StateApx* self) {
    free(self->buckets);
    self->buckets = NULL;
}

// 合并相同大小的桶（最多保留 k + 1 个，从新->旧）
void merge_buckets(StateApx* self) {
    int i = self->current_buckets - 1; // 从最新桶开始（数组末尾）
    while (i >= 2) { // 至少需要三个桶才能合并
        // 检查从最新到最旧的连续桶是否大小相同
        int count = 1;
        int j = i - 1;
        while (j >= 0 && self->buckets[j].size == self->buckets[i].size) {
            count++;
            j--;
        }
        if (count >= self->k + 2) { // 如果数量达到 k + 2，则合并最老的两个桶
            // 找到最老的两个桶（j + 1 和 j + 2）
            int oldest_idx = j + 1; // 最老的桶
            int next_oldest_idx = j + 2; // 第二老的桶

            // 合并最老的两个桶
            self->buckets[oldest_idx].size *= 2; // 大小翻倍
            self->buckets[oldest_idx].timestamp = self->buckets[next_oldest_idx].timestamp; // 保留较新的时间戳

            // 移除第二老的桶，向前移动后续桶
            for (int m = next_oldest_idx; m < self->current_buckets - 1; m++) {
                self->buckets[m] = self->buckets[m + 1];
            }
            self->current_buckets--;
            self->buckets[self->current_buckets].size = 0; // 标记为未使用
            self->buckets[self->current_buckets].timestamp = -1;

            // 重新从最新桶开始检查（可能触发级联合并）
            i = self->current_buckets - 1;
        } else {
            i--; // 继续向左检查
        }
    }
}




uint32_t wnd_bit_count_apx_next(StateApx* self, bool item) {
    self->current_time++;

//每加入一个新元素，移除过期桶+更新桶的逻辑(使用循环缓冲区优化？)
    // 移除过期桶（从开头检查）
    while (self->current_buckets > 0 && (self->current_time - self->buckets[0].timestamp >= self->window_size)) {
        for (int i = 0; i < self->current_buckets - 1; i++) {
            self->buckets[i] = self->buckets[i + 1];
        }
        self->current_buckets--;
        self->buckets[self->current_buckets].size = 0;
        self->buckets[self->current_buckets].timestamp = -1;
        N_MERGES++;
    }
    // 如果新比特是1，添加新桶到末尾
    if (item == 1 && self->current_buckets < self->max_buckets) {
        self->buckets[self->current_buckets].size = 1;
        self->buckets[self->current_buckets].timestamp = self->current_time;
        self->current_buckets++;
        merge_buckets(self); // 检查并合并
    }


// 估计窗口内1的数量
    self->total_ones = 0;
    self->oldest_valid_index = -1;
    // 遍历所有桶，累加有效桶的size
    for (int i = 0; i < self->current_buckets; i++) {
        if (self->current_time - self->buckets[i].timestamp < self->window_size) {
            self->total_ones += self->buckets[i].size;
            if (self->oldest_valid_index == -1) {
                self->oldest_valid_index = i; // 记录最老的有效桶
            }
        }
    }
    // 减去最老有效桶size的一半以减少误差
    if (self->oldest_valid_index != -1) {
        self->total_ones -= self->buckets[self->oldest_valid_index].size ;
        self->total_ones ++;
    }
    printf("第%i个,值 %u: 估计1的个数 = %u, 当前桶数 = %u\n", self->current_time-1,
        item, self->total_ones, self->current_buckets);

    return self->total_ones;
}

void wnd_bit_count_apx_print(StateApx* self) {
    // 打印state

}