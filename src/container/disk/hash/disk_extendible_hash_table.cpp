//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) 
      {
    BasicPageGuard header_page_guard = bpm_->NewPageGuarded(&header_page_id_);
    WritePageGuard header_page_guard_write = header_page_guard.UpgradeWrite();
  //  Init header page
    ExtendibleHTableHeaderPage* header_page_data = header_page_guard_write.AsMut<ExtendibleHTableHeaderPage>();
    header_page_data->Init(header_max_depth);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool 
    {
    uint32_t hash = Hash(key);

    // Fetch and use header page
    auto header_guard = bpm_->FetchPageRead(header_page_id_);
    const ExtendibleHTableHeaderPage* header_page = header_guard.As<ExtendibleHTableHeaderPage>();

    uint32_t dir_ind = header_page->HashToDirectoryIndex(hash);
    uint32_t dir_page_id_ = header_page->GetDirectoryPageId(dir_ind);

    // Drop the header page guard
    header_guard.Drop();

    if (dir_page_id_ == 0) return false;

    // Fetch and use directory page
    auto dir_guard = bpm_->FetchPageRead(dir_page_id_);
    const ExtendibleHTableDirectoryPage* dir_page = dir_guard.As<ExtendibleHTableDirectoryPage>();

    uint32_t buc_ind = dir_page->HashToBucketIndex(hash);
    uint32_t buc_page_id_ = dir_page->GetBucketPageId(buc_ind);

    // Drop the directory page guard
    dir_guard.Drop();

    // Fetch and use bucket page
    auto buc_guard = bpm_->FetchPageRead(buc_page_id_);
    const ExtendibleHTableBucketPage<K, V, KC>* buc_page = buc_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();

    // Drop the bucket page guard
    buc_guard.Drop();
    V value;
    if(!buc_page->Lookup(key, value, cmp_)) return false;
    result->push_back(value);
    return true;
  }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool 
{
  uint32_t hash = Hash(key);
  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  ExtendibleHTableHeaderPage* header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  uint32_t directory_idx = header_page->HashToDirectoryIndex(hash);
  page_id_t dir_page_id_ = header_page->GetDirectoryPageId(directory_idx);
  if(dir_page_id_==0){
    return InsertToNewDirectory(header_page, directory_idx, hash, key, value);
  }
  header_guard.Drop();

  WritePageGuard dir_guard = bpm_->FetchPageWrite(dir_page_id_);
  ExtendibleHTableDirectoryPage* dir_page = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();
  uint32_t buc_ind = dir_page->HashToBucketIndex(hash);
  uint32_t buc_page_id_ = dir_page->GetBucketPageId(buc_ind);
  if(buc_page_id_==0){
    return InsertToNewBucket(dir_page, buc_ind, key, value);
    }
  WritePageGuard buc_guard = bpm_->FetchPageWrite(buc_page_id_); 
  ExtendibleHTableBucketPage<K, V, KC> * buc_page = buc_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  while(buc_page->IsFull())
  {
    dir_page->IncrLocalDepth(buc_ind);
    buc_guard.Drop();
    buc_ind = dir_page->HashToBucketIndex(hash);
    buc_page_id_ = dir_page->GetBucketPageId(buc_ind);
    if(buc_page_id_==0){
      InsertToNewBucket(dir_page, buc_ind, key, value);
      break;
    }
    buc_guard.Drop();
    buc_guard = bpm_->FetchPageWrite(buc_page_id_);
    buc_page = buc_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  }
  buc_page->Insert(key, value, cmp_);
  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t* directory_page_id_ptr= new page_id_t(0);
  BasicPageGuard directory_page_guard = bpm_->NewPageGuarded(directory_page_id_ptr);
  if(*directory_page_id_ptr==0) return false;
  header->SetDirectoryPageId(directory_idx, *directory_page_id_ptr);
  WritePageGuard write_dir_guard = directory_page_guard.UpgradeWrite();
  ExtendibleHTableDirectoryPage * dir_page = write_dir_guard.AsMut<ExtendibleHTableDirectoryPage>();
  dir_page->Init(directory_max_depth_);
  uint32_t bucket_idx = dir_page->HashToBucketIndex(hash);
  return InsertToNewBucket(dir_page, bucket_idx, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool 
  {
  page_id_t* bucket_page_id_ptr = new page_id_t(0);
  BasicPageGuard bucket_page_guard = bpm_->NewPageGuarded(bucket_page_id_ptr);
  if(*bucket_page_id_ptr==0) return false;
  directory->SetBucketPageId(bucket_idx, *bucket_page_id_ptr);
  WritePageGuard write_buc_guard = bucket_page_guard.UpgradeWrite();
  ExtendibleHTableBucketPage<K, V, KC> * buc_page = write_buc_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  buc_page->Init(bucket_max_size_);
  return buc_page->Insert(key, value, cmp_);
  }

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::IncrLocalDepth(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx) -> bool
{
  ExtendibleHTableBucketPage<K, V, KC> * old_buc_page = bpm_->FetchPageWrite(directory->GetBucketPageId(bucket_idx)).AsMut<ExtendibleHTableBucketPage<K, V, KC>>();  

  uint32_t ld = directory->GetLocalDepth(bucket_idx);
  uint32_t mask = 1<<(ld+1);
  page_id_t old_page_id = directory->GetBucketPageId(bucket_idx);
  page_id_t * buc1_page_id_ptr = new page_id_t(0);
  page_id_t * buc2_page_id_ptr = new page_id_t(0);
  
  BasicPageGuard buc1_page_guard = bpm_->NewPageGuarded(buc1_page_id_ptr);
  BasicPageGuard buc2_page_guard = bpm_->NewPageGuarded(buc2_page_id_ptr);
  if((*buc1_page_id_ptr == 0) || (*buc2_page_id_ptr == 0)) return false;
  WritePageGuard write_buc1_page_guard = buc1_page_guard.UpgradeWrite();
  WritePageGuard write_buc2_page_guard = buc2_page_guard.UpgradeWrite();
  ExtendibleHTableBucketPage<K, V, KC> * buc1_page = write_buc1_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  ExtendibleHTableBucketPage<K, V, KC> * buc2_page = write_buc2_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  // Redistribute data
  for(size_t idx=0; idx<old_buc_page->Size(); idx++){
    auto [key, value] = old_buc_page->EntryAt(idx);
    uint32_t hash = Hash(key);
    if(hash & mask) buc2_page->Insert(key, value, cmp_);
    else buc1_page->Insert(key, value, cmp_);
  }

  //Redistribute page pointer
  for(size_t idx=0; idx<directory->Size(); idx++){
    if(directory->GetBucketPageId(idx)==old_page_id){
      directory->IncrLocalDepth(idx);
      if(idx & mask) directory->SetBucketPageId(idx, *buc2_page_id_ptr);
      else directory->SetBucketPageId(idx, *buc1_page_id_ptr);
    }
  }
  return true;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool 
{
  int32_t hash = Hash(key);
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  const ExtendibleHTableHeaderPage* header_page = header_guard.As<ExtendibleHTableHeaderPage>();
  uint32_t directory_idx = header_page->HashToDirectoryIndex(hash);
  page_id_t dir_page_id_ = header_page->GetDirectoryPageId(directory_idx);
  header_guard.Drop();
  if(dir_page_id_==0){
    return false;
  }
  WritePageGuard dir_guard = bpm_->FetchPageWrite(dir_page_id_);
  ExtendibleHTableDirectoryPage* dir_page = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_idx = dir_page->HashToBucketIndex(hash);
  page_id_t buc_page_id_ = dir_page->GetBucketPageId(bucket_idx);
  if(buc_page_id_==0){
    return false;
  }
  WritePageGuard buc_guard = bpm_->FetchPageWrite(buc_page_id_);
  ExtendibleHTableBucketPage<K, V, KC> * buc_page = buc_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if(!buc_page->Remove(key, cmp_)) return false;
  while(buc_page->IsEmpty()){
    uint32_t ld = dir_page->GetLocalDepth(bucket_idx);
    if(ld==0) break;
    uint32_t new_buc_idx = bucket_idx ^ (1<<ld);
    if(ld == dir_page->GetLocalDepth(new_buc_idx))
    {
      page_id_t new_page_id = dir_page->GetBucketPageId(new_buc_idx);
      for(size_t idx=0; idx<dir_page->Size(); idx++){
        if(dir_page->GetBucketPageId(idx)==buc_page_id_ || dir_page->GetBucketPageId(idx)==new_page_id){
          dir_page->SetBucketPageId(idx, new_page_id);
          dir_page->DecrLocalDepth(idx);
        }
      }
      
      bucket_idx = new_buc_idx;
      buc_guard.Drop();
      buc_guard = bpm_->FetchPageWrite(new_page_id);
      buc_page = buc_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    }
    else{
      break;
    }
      
  }
  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
