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

BasicPageGuard::UpgradeRead(){
    return ReadPageGuard(this->bpm_, this->page_)
}

BasicPageGuard::UpgradeWrite(){
    return WritePageGuard(this->bpm_, this->page_)
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept
{
    this->guard_ = that->guard_;
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & 
{ 
    this->guard_ = that->guard_;
    return *this; 
}

void ReadPageGuard::Drop() 
{
    this->guard_.page_.WLatch();
    this->guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() 
{
    this->Drop();
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & { return *this; }

void WritePageGuard::Drop() {}

WritePageGuard::~WritePageGuard() {}  // NOLINT

}  // namespace bustub
