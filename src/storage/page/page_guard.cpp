#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept 
{
    this->page_ = that->page_;
    this->bpm_ = that->bpm_;
    that->page_ = nullptr;
    that->bpm_ = nullptr;    
    return *this; 
}

void BasicPageGuard::Drop() 
{
    if(this->page_!=nullptr) this->bpm_->UnpinPage(PageId(), is_dirty_);
    that->page_ = nullptr;
    that->bpm_ = nullptr; 
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & 
{
    this->page_ = that->page_;
    this->bpm_ = that->bpm_;
    that->page_ = nullptr;
    that->bpm_ = nullptr;    
    return *this; 
}

BasicPageGuard::~BasicPageGuard()
{
    this->Drop();
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & { return *this; }

void ReadPageGuard::Drop() {}

ReadPageGuard::~ReadPageGuard() {}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & { return *this; }

void WritePageGuard::Drop() {}

WritePageGuard::~WritePageGuard() {}  // NOLINT

}  // namespace bustub
