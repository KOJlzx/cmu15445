#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
    that.bpm_ = nullptr;
    that.page_ = nullptr;
}

// Drop函数实现，负责unpin页面
void BasicPageGuard::Drop() {
    if (bpm_ == nullptr || page_ == nullptr) {
        return;
    }
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
    bpm_ = nullptr;
    page_ = nullptr;
}

// 移动赋值运算符
auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & { 
    if (this != &that) {
      Drop();  // 确保当前资源被释放
      bpm_ = that.bpm_;
      page_ = that.page_;
      is_dirty_ = that.is_dirty_;

      that.bpm_ = nullptr;
      that.page_ = nullptr;
      return *this;
    }

    return *this; 
}

// 析构函数，调用Drop确保资源释放
BasicPageGuard::~BasicPageGuard(){ Drop(); };  // NOLINT

// 提升为ReadPageGuard，并锁定页面的读锁
auto BasicPageGuard::UpgradeRead() -> ReadPageGuard { 
    if (bpm_ == nullptr || page_ == nullptr) {
        return {};
    }

    page_->RLatch();  // 读锁定页面
    auto guard = ReadPageGuard{bpm_, page_};  // 返回读页面保护器
    bpm_ = nullptr;  // 防止重复操作
    page_ = nullptr;
    return guard; 
}

// 提升为WritePageGuard，并锁定页面的写锁
auto BasicPageGuard::UpgradeWrite() -> WritePageGuard { 
    if (bpm_ == nullptr || page_ == nullptr) {
        return {};
    }

    page_->WLatch();  // 写锁定页面
    auto guard = WritePageGuard{bpm_, page_};  // 返回写页面保护器
    bpm_ = nullptr;  // 防止重复操作
    page_ = nullptr;
    return guard; 
}

ReadPageGuard::ReadPageGuard(BufferPoolManager *bpm, Page *page) {}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & { \
    if (this == &that) {
        return *this;
    }

    Drop();
    guard_ = std::move(that.guard_);
    return *this; 
}

void ReadPageGuard::Drop() {
    if (guard_.bpm_ == nullptr || guard_.page_ == nullptr) {
        return;
    }
  
    guard_.page_->RUnlatch();  // 解锁读锁
    guard_.Drop();  // 调用基础页面保护器的Drop
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(BufferPoolManager *bpm, Page *page) {}

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & { 
    if (this == &that) {
        return *this;
    }

    Drop();
    guard_ = std::move(that.guard_);
    return *this;
}

// Drop函数实现，释放写锁
void WritePageGuard::Drop() {
    if (guard_.bpm_ == nullptr || guard_.page_ == nullptr) {
        return;
    }

    guard_.page_->WUnlatch();  // 解锁写锁
    guard_.Drop();  // 调用基础页面保护器的Drop
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
