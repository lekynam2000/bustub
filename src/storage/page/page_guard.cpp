#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept 
{
    this->page_ = that.page_;
    this->bpm_ = that.bpm_;
    that.page_ = nullptr;
    that.bpm_ = nullptr;    
}

void BasicPageGuard::Drop() 
{
    if(this->page_!=nullptr) this->bpm_->UnpinPage(PageId(), is_dirty_);
    this->page_ = nullptr;
    this->bpm_ = nullptr; 
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & 
{
    this->page_ = that.page_;
    this->bpm_ = that.bpm_;
    that.page_ = nullptr;
    that.bpm_ = nullptr;    
    return *this; 
}

BasicPageGuard::~BasicPageGuard()
{
    this->Drop();
};  // NOLINT

ReadPageGuard BasicPageGuard::UpgradeRead(){
    ReadPageGuard temp = ReadPageGuard(this->bpm_, this->page_);
    this->page_->RLatch();
    this->bpm_ = nullptr;
    this->page_ = nullptr;
    return temp;
}

WritePageGuard BasicPageGuard::UpgradeWrite(){
    WritePageGuard temp = WritePageGuard(this->bpm_, this->page_);
    this->page_->WLatch();
    this->bpm_ = nullptr;
    this->page_ = nullptr;
    return temp;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept
{
    this->guard_ = std::move(that.guard_);
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & 
{ 
    this->guard_ = std::move(that.guard_);
    
    return *this; 
}

void ReadPageGuard::Drop() 
{
    if(this->guard_.page_!=nullptr) this->guard_.page_->RUnlatch();
    this->guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() 
{
    this->Drop();
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept
{
    this->guard_ = std::move(that.guard_);
    this->guard_.is_dirty_ = true;
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & 
{ 
    this->guard_ = std::move(that.guard_);
    return *this; 
}

void WritePageGuard::Drop() 
{
    if(this->guard_.page_!=nullptr) this->guard_.page_->WUnlatch();
    this->guard_.Drop();
}

WritePageGuard::~WritePageGuard() 
{  // NOLINT
    this->Drop();
}  // namespace bustub
}