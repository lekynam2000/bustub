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
    -> bool {
    uint32_t hash = Hash(key);
    const ExtendibleHTableHeaderPage* header_page = bpm_->FetchPageRead(header_page_id_).As<ExtendibleHTableHeaderPage>();
    uint32_t dir_ind = header_page->HashToDirectoryIndex(hash);
    uint32_t dir_page_id_ = header_page->GetDirectoryPageId(dir_ind);
    if(dir_page_id_==0) return false;
    const ExtendibleHTableDirectoryPage* dir_page = bpm_->FetchPageRead(dir_page_id_).As<ExtendibleHTableDirectoryPage>();
    uint32_t buc_ind = dir_page->HashToBucketIndex(hash);
    uint32_t buc_page_id_ = dir_page->GetBucketPageId(buc_ind);
    const ExtendibleHTableBucketPage<K, V, KC> * buc_page = bpm_->FetchPageRead(buc_page_id_).As<ExtendibleHTableBucketPage<K, V, KC>>();
    return buc_page->Lookup(key, *(result->data()), cmp_);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  uint32_t hash = Hash(key);
  ExtendibleHTableHeaderPage* header_page = bpm_->FetchPageWrite(header_page_id_).AsMut<ExtendibleHTableHeaderPage>();
  uint32_t directory_idx = header_page->HashToDirectoryIndex(hash);
  page_id_t dir_page_id_ = header_page->GetDirectoryPageId(directory_idx);
  if(dir_page_id_==0){
    return InsertToNewDirectory(header_page, directory_idx, hash, key, value);
  }
  ExtendibleHTableDirectoryPage* dir_page = bpm_->FetchPageWrite(dir_page_id_).AsMut<ExtendibleHTableDirectoryPage>();
  uint32_t buc_ind = dir_page->HashToBucketIndex(hash);
  uint32_t buc_page_id_ = dir_page->GetBucketPageId(buc_ind);
  if(buc_page_id_==0){
    InsertToNewBucket(dir_page, buc_ind, key, value);
  }
  ExtendibleHTableBucketPage<K, V, KC> * buc_page = bpm_->FetchPageWrite(buc_page_id_).AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  while(buc_page->IsFull())
  {
    dir_page->IncrLocalDepth(buc_ind);
    bpm_->UnpinPage(buc_page, true);
    buc_ind = dir_page->HashToBucketIndex(hash);
    buc_page_id_ = dir_page->GetBucketPageId(buc_ind);
    if(buc_page_id_==0){
      InsertToNewBucket(dir_page, buc_ind, key, value);
      break;
      }
    buc_page = bpm_->FetchPageWrite(buc_page_id_).AsMut<ExtendibleHTableBucketPage<K, V, KC>>(); 
  }
  buc_page->Insert(key, value, cmp_);
  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t* directory_page_id_= nullptr;
  BasicPageGuard directory_page_guard = bpm_->NewPageGuarded(directory_page_id_);
  if(directory_page_id_==nullptr) return false;
  header->SetDirectoryPageId(directory_idx, *directory_page_id_);
  ExtendibleHTableDirectoryPage * dir_page = directory_page_guard.UpgradeWrite().AsMut<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_idx = dir_page->HashToBucketIndex(hash);
  return InsertToNewBucket(dir_page, bucket_idx, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t* bucket_page_id_=nullptr;
  BasicPageGuard bucket_page_guard = bpm_->NewPageGuarded(bucket_page_id_);
  if(bucket_page_id_==nullptr) return false;
  directory->SetBucketPageId(bucket_idx, *bucket_page_id_);
  ExtendibleHTableBucketPage<K, V, KC> * buc_page = bucket_page_guard.UpgradeWrite().AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  buc_page->Insert(key, value, cmp_);
  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::IncrLocalDepth(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx) -> bool{
  ExtendibleHTableBucketPage<K, V, KC> * old_buc_page = bpm_->FetchPageWrite(directory->GetBucketPageId(bucket_idx)).AsMut<ExtendibleHTableBucketPage<K, V, KC>>();  

  uint32_t ld = directory->GetLocalDepth(bucket_idx);
  uint32_t mask = 1<<(ld+1);
  page_id_t old_page_id = directory->GetBucketPageId(bucket_idx);
  page_id_t * buc1_page_id_ptr = nullptr;
  page_id_t * buc2_page_id_ptr = nullptr;
  
  BasicPageGuard buc1_page_guard = bpm_->NewPageGuarded(buc1_page_id_ptr);
  BasicPageGuard buc2_page_guard = bpm_->NewPageGuarded(buc2_page_id_ptr);
  if((buc1_page_id_ptr == nullptr) || (buc2_page_id_ptr == nullptr)) return false;
  
  ExtendibleHTableBucketPage<K, V, KC> * buc1_page = buc1_page_guard.UpgradeWrite().AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  ExtendibleHTableBucketPage<K, V, KC> * buc2_page = buc2_page_guard.UpgradeWrite().AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  // Redistribute data
  for(size_t idx=0; idx<old_buc_page->Size(); idx++){
    auto [key, value] = old_buc_page->EntryAt(idx);
    uint32_t hash = Hash(key);
    if(hash & mask) buc2_page->Insert(key, value, cmp_);
    else buc1_page->Insert(key, value, cmp_);
  }

  //Redistribute page pointer
  for(size_t idx=0; idx<directory->Size(); idx++){
    if(directory->bucket_page_ids_[idx]==old_page_id){
      if(idx & mask) directory->bucket_page_ids_[idx] = *buc2_page_id_ptr;
      else directory->bucket_page_ids_[idx] = *buc1_page_id_ptr;
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
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  return false;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
