//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  frame_latch_ = new std::mutex[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * { 
  std::unique_lock<std::mutex> lock(latch_);
  frame_id_t inserted_frame;
  if(free_list_.size()>0){
    inserted_frame = free_list_.front();
    free_list_.pop_front();
    
  }else{
    
    if(!replacer_->Evict(&inserted_frame)) return nullptr;
  }
  auto new_page_id = AllocatePage();
  page_table_[new_page_id] = inserted_frame;
  lock.unlock();

  std::unique_lock<std::mutex> frame_lock(frame_latch_[inserted_frame]);
  replacer_->RecordAccess(inserted_frame);
  replacer_->SetEvictable(inserted_frame, false);
  
  *page_id = new_page_id;
  if(pages_[inserted_frame].GetPageId() != INVALID_PAGE_ID){
    FlushPage(pages_[inserted_frame].GetPageId());
    page_table_.erase(pages_[inserted_frame].GetPageId());
    }
  pages_[inserted_frame].ResetMemory();
  pages_[inserted_frame].pin_count_=1; 
  pages_[inserted_frame].is_dirty_=true;
  pages_[inserted_frame].page_id_=new_page_id;
  return &pages_[inserted_frame];
   }

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::unique_lock<std::mutex> lock(latch_);
  if(page_table_.find(page_id)==page_table_.end()){
    frame_id_t inserted_frame;
    if(free_list_.size()>0){
      inserted_frame = free_list_.front();
      free_list_.pop_front();
    }else{
      if(!replacer_->Evict(&inserted_frame)) return nullptr;
    }
    page_table_[page_id] = inserted_frame;
    lock.unlock();

    std::unique_lock<std::mutex> frame_lock(frame_latch_[inserted_frame]);
    replacer_->RecordAccess(inserted_frame);
    replacer_->SetEvictable(inserted_frame, false);

    //Get page content from disk and write to buf
    char buf[BUSTUB_PAGE_SIZE] = {0};
    auto disk_promise = disk_scheduler_->CreatePromise();
    auto disk_future = disk_promise.get_future();
    disk_scheduler_->Schedule({false, reinterpret_cast<char *>(&buf), page_id, std::move(disk_promise)});
    disk_future.get();
    
    //Refresh mem for inserted page and write buf to the memory page
    if(pages_[inserted_frame].GetPageId() != INVALID_PAGE_ID){
      FlushPage(pages_[inserted_frame].GetPageId());
      page_table_.erase(pages_[inserted_frame].GetPageId());
      }
    pages_[inserted_frame].ResetMemory();
    pages_[inserted_frame].page_id_=page_id;
    pages_[inserted_frame].pin_count_=1;
    pages_[inserted_frame].is_dirty_ = false;
    std::memcpy(pages_[inserted_frame].data_, buf, BUSTUB_PAGE_SIZE);
    return &pages_[inserted_frame];
  }
  
  auto inserted_frame = page_table_[page_id];
  pages_[inserted_frame].pin_count_+=1;
  lock.unlock();
  return &pages_[inserted_frame];
  
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  if(page_table_.find(page_id)==page_table_.end()) return false;
  auto frame = page_table_[page_id];
  if(pages_[frame].pin_count_<=0) return false;
  pages_[frame].pin_count_--;
  if(pages_[frame].pin_count_==0){
    replacer_->SetEvictable(frame, true);
  }
  pages_[frame].is_dirty_=is_dirty;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool 
{
  std::unique_lock<std::mutex> lock(latch_);
  if(page_table_.find(page_id)==page_table_.end()) return false;
  auto frame = page_table_[page_id];
  
  //write page content to disk
  auto disk_promise = disk_scheduler_->CreatePromise();
  auto disk_future = disk_promise.get_future();
  disk_scheduler_->Schedule({true, pages_[frame].data_, page_id, std::move(disk_promise)});
  disk_future.get();
  
  pages_[frame].is_dirty_=false;

  return true; 
  
}

void BufferPoolManager::FlushAllPages() 
{
  std::unique_lock<std::mutex> lock(latch_);
  for(const auto& pair: page_table_){
    auto frame = pair.second;
    //write page content to disk
    auto disk_promise = disk_scheduler_->CreatePromise();
    auto disk_future = disk_promise.get_future();
    disk_scheduler_->Schedule({true, pages_[frame].data_, pair.first, std::move(disk_promise)});
    disk_future.get();
    
    pages_[frame].is_dirty_=false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool 
{ 
  if(page_table_.find(page_id)!=page_table_.end()){
    auto frame = page_table_[page_id];
    if(pages_[frame].GetPinCount()>0) return false;
    DeallocatePage(page_id);
    replacer_->Remove(frame);
    free_list_.push_back(frame);
    pages_[page_id].ResetMemory();
  }
  return true; 
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return BasicPageGuard(this, this->FetchPage(page_id)); }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return FetchPageBasic(page_id).UpgradeRead(); }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return FetchPageBasic(page_id).UpgradeWrite(); }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return BasicPageGuard(this, this->NewPage(page_id)); }

}  // namespace bustub
