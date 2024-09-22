//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"
#include <optional> 
namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { 
    // 加锁，确保线程安全，避免多个线程同时操作LRUKReplacer中的数据
    std::lock_guard<std::mutex> guard(latch_);


    // 定义一个optional类型变量 victim，用来存储最终要被淘汰的 frame_id
    std::optional<frame_id_t> victim;

    // 追踪最早的时间戳，用于在多个 frame 的 K 距离相同的情况下进行比较
    size_t oldest_timestamp = std::numeric_limits<size_t>::max();
    
    size_t max_k_distance = 0;

    
    // 遍历每个存储的页面(frame)节点，找到最合适的淘汰目标
    for (auto &[frame_id, node] : node_store_) {
        // 如果该 frame 不可淘汰，则跳过
        if (!node.is_evictable_) {
            continue;
        }

        // 计算 K 距离
        // 如果 history_ 的大小大于等于 k，说明我们有足够的访问历史来计算 K 距离
        size_t k_distance = (node.history_.size() >= k_)
                            // K 距离 = 当前时间戳 - K次前的访问时间戳
                            ? current_timestamp_ - node.history_.front()
                            // 如果历史记录少于K次，则将K距离设为无穷大(+inf)
                            : std::numeric_limits<size_t>::max();
        // 更新最大 K 距离的 frame
        // 如果当前的 K 距离比记录的最大 K 距离大，或者 K 距离相同但页面访问的时间戳更早（LRU策略）
        if (k_distance > max_k_distance || 
            (k_distance == max_k_distance && node.history_.front() < oldest_timestamp)) {
            // 更新最大 K 距离
            max_k_distance = k_distance;

            // 更新最早时间戳
            oldest_timestamp = node.history_.front();

            // 记录下此页面为候选淘汰目标
            victim = frame_id;
        }
    }

    // 如果找到一个可淘汰的页面
    if (victim.has_value()) {
        *frame_id = victim.value();
        node_store_.erase(victim.value());
        // 当前可淘汰页面的数量减1
        --curr_size_;
        
        return true;
    }


    // 返回 victim，它是 optional 类型，可能包含被淘汰的 frame_id 或者为空
    return false; 
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    std::lock_guard<std::mutex> guard(latch_);

    if (static_cast<size_t>(frame_id) > replacer_size_) {
        throw bustub::Exception("Invalid frame id");
    }


    // 增加时间戳
    current_timestamp_++;

    // 有没有在缓冲池中找到该页面, 没有则加入
    if (node_store_.find(frame_id) == node_store_.end()){
        node_store_[frame_id] = LRUKNode{};
    }
    // 从node_store取出当前要访问的页面
    auto &node = node_store_[frame_id];
    
    
    // 将当前的时间戳push到访问的页面中
    node.history_.push_back(current_timestamp_);

    // 记录每个页面的访问时间戳。如果该页面在`k`个访问记录以上，则删除最早的访问记录。
    if (node.history_.size() > k_) {
        node.history_.pop_front();
    }
    node.fid_ = frame_id;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::lock_guard<std::mutex> guard(latch_);

    // 可以使用 static_cast 进行显式转换
    if (static_cast<size_t>(frame_id) > replacer_size_) {
        throw bustub::Exception("Invalid frame id");
    }

    if(node_store_.find(frame_id) == node_store_.end()){
        return;
    }

    auto &node = node_store_[frame_id];

    // 设置的和原来的状态不一样才改
    if (node.is_evictable_ != set_evictable) {
        node.is_evictable_ = set_evictable;
        curr_size_ += (set_evictable ? 1 : -1);
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::lock_guard<std::mutex> guard(latch_);

    if (static_cast<size_t>(frame_id) > replacer_size_) {
        throw bustub::Exception("Invalid frame id");
    }

    auto it = node_store_.find(frame_id);

    // 判断是否找到当前页面
    if (it != node_store_.end()) {
        //如果当前页面是不可淘汰的状态, 抛出错误
        if (!it->second.is_evictable_) {
            throw bustub::Exception("Attempt to remove a non-evictable frame");
        }

        node_store_.erase(it);
        --curr_size_;
    }    
}

auto LRUKReplacer::Size() -> size_t { 
    std::lock_guard<std::mutex> guard(latch_);
    return curr_size_;    
}

}  // namespace bustub
