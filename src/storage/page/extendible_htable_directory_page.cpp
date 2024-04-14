//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth): max_depth(max_depth) 
{
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t 
{ 
  return hash<<((1<<global_depth_)-1);
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t 
{ 
  return bucket_page_ids_[bucket_idx]; 
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) 
{
  bucket_page_ids[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t 
{ 
   return bucket_idx+1;
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t 
{ 
  return global_depth_; 
}

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() 
{
  global_depth_+=1;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() 
{
  global_depth_-=1;
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool 
{ 
  for(int i=0;i<HTABLE_DIRECTORY_ARRAY_SIZE;i++){
    if(local_depths_[i]>=max_depth_){
      return false;
    }
  }
  return true; 
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t 
{ 
  uint32_t sum=0;
  for(int i=0;i<HTABLE_DIRECTORY_ARRAY_SIZE;i++){
    if(local_depths_[i]>0 && !(i<<local_depths_[i])){
      sum+=1;
    }
  } 
  return sum;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t 
{ 
  return local_depths_[bucket_idx]; 
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) 
{
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) 
{
  local_depths_[bucket_idx]++;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) 
{
  local_depths_[bucket_idx]--;
}

}  // namespace bustub
