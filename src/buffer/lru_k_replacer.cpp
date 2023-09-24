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

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { 
    std::lock_guard<std::mutex> lock(latch_);
    auto toRemoveIt = listRecentKth_.begin();
    while(toRemoveIt!=listRecentKth_.end() && !node_store_[*toRemoveIt].is_evictable){
      toRemoveIt++;
    }  
    if(toRemoveIt==listRecentKth_.end()) return false;

    *frame_id  = *toRemoveIt;

    //Remove that frame_id everywhere
    if(endInfIt_ == toRemoveIt){
      endInfIt_ = std::next(toRemoveIt);
    }
    mapRecentKth_.erase(*frame_id);
    listRecentKth_.erase(toRemoveIt);
    node_store_.erase(*frame_id);
    curr_size_--;
    return true;
 }

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    BUSTUB_ASSERT(frame_id<(frame_id_t)replacer_size_, true);
    std::lock_guard<std::mutex> lock(latch_);
    current_timestamp_++;
    if(node_store_.find(frame_id)==node_store_.end()){
      node_store_.emplace(frame_id,k_);
      listRecentKth_.push_back(frame_id);
      mapRecentKth_[frame_id]=std::prev(listRecentKth_.end());
    }
    if(node_store_[frame_id].saveHistory(current_timestamp_)&& mapRecentKth_[frame_id]!=listRecentKth_.end()){
      //End of inf iterator shift to the next if the current one need to move
      if(mapRecentKth_[frame_id]==endInfIt_ ){
        endInfIt_ = std::next(endInfIt_);
      }
      //If newest node got k historical access, put to the end
      listRecentKth_.splice(listRecentKth_.end(), listRecentKth_, mapRecentKth_[frame_id]);
    }
    else{
      listRecentKth_.splice(endInfIt_, listRecentKth_, mapRecentKth_[frame_id]);
    }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    BUSTUB_ASSERT(frame_id<(frame_id_t)replacer_size_, true);
    std::lock_guard<std::mutex> lock(latch_);
    if(node_store_.find(frame_id)!=node_store_.end()){
      int source = node_store_[frame_id].is_evictable;
      int target = set_evictable;
      node_store_[frame_id].is_evictable = set_evictable;
      curr_size_+=target-source;
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::lock_guard<std::mutex> lock(latch_);
    if(node_store_.find(frame_id)==node_store_.end()) return;
    BUSTUB_ASSERT(node_store_[frame_id].is_evictable, true);
    auto toRemoveIt = mapRecentKth_[frame_id];
    if(toRemoveIt==endInfIt_){
      endInfIt_ = std::next(endInfIt_);
    }
    listRecentKth_.erase(toRemoveIt);
    mapRecentKth_.erase(frame_id);
    node_store_.erase(frame_id);
    curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
