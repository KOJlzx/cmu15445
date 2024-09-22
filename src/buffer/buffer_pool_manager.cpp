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
  // TODO(students): remove this line after you have implemented the buffer pool manager
  /*throw NotImplementedException(
      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
      "exception line in `buffer_pool_manager.cpp`.");*/

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  // 查看是否有空闲的page
  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Evict(&frame_id)) {
    // 如果没有页面可以置换的话就返回空
    return nullptr;
  }
  else {
    // lruk 换出来  需要更新页表  同时要将它写回磁盘
    page_table_.erase(pages_[frame_id].GetPageId());
    if(pages_[frame_id].IsDirty()) {
      WriteFrame(frame_id, pages_[frame_id].GetPageId());
    }
  }
  *page_id = AllocatePage(); // 分配一个page_id

  Page *page = &pages_[frame_id]; // 当前页框


  page->ResetMemory(); // 重置当前页框
  // 给这个页分配一个新的page 并且pin这个page

  page->page_id_ = *page_id;

  page->pin_count_ = 1;  // Pin 这个页           
  page->is_dirty_ = false;

  // 更新页表和replacer_
  page_table_[*page_id] = frame_id;
  replacer_->SetEvictable(frame_id, false);
  replacer_->RecordAccess(frame_id);

  return page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  
  std::scoped_lock<std::mutex> lock(latch_);
  
  // 如果调用页面是否出现在页表（缓冲池）中
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    Page *page = &pages_[frame_id];

    // 给这个进程pin，说明有一个任务
    if (page->pin_count_ == 0) {
      replacer_->SetEvictable(frame_id, false);
    }
    page->pin_count_++;
    replacer_->RecordAccess(frame_id, access_type);
    return page;
  }
  //  如果不在页表中则需要向磁盘中调用
  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Evict(&frame_id)) {
    return nullptr; // 没有空闲页和可替换页
  } else {
    // lruk 换出来  需要更新页表  同时要将它写回磁盘
    page_table_.erase(pages_[frame_id].GetPageId());
    if(pages_[frame_id].IsDirty()) {
      WriteFrame(frame_id, pages_[frame_id].GetPageId());
    }
  }

  // 如果当前page是脏的就写回磁盘中去
  Page *page = &pages_[frame_id];

  // 从磁盘中读取数据
  page->ResetMemory();

  ReadFrame(frame_id, page_id);

  // 更新该页面并且pin该页面
  page_table_[page_id] = frame_id;
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;

  replacer_->RecordAccess(frame_id, access_type);
  replacer_->SetEvictable(frame_id, false);

  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  
  // 查找页面是否在缓冲池里
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    // 如果在缓冲池里没找到该页面直接返回false
    return false;
  }
  
  //找到页框号
  frame_id_t frame_id = it->second;
  Page *page = &pages_[frame_id];

  if (page->pin_count_ <= 0) {
    // 已经unpined 不用再unpin这个进程
    return false;
  }  

  // 如果要更新成脏页就更新
  page->is_dirty_ = is_dirty || page->is_dirty_;
  page->pin_count_--;

  // 如果pin_count_==0，让replacer_设置为evictable
  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}


auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { 
  std::lock_guard<std::mutex> guard(latch_);

  // 查找页面是否在缓冲池里
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    // 如果在缓冲池里没找到该页面直接返回false
    return false;
  }

  frame_id_t frame_id = it->second;
  Page *page = &pages_[frame_id];

  // 将页面写回到磁盘，同时并将该 page 标记为 non-dirty

  WriteFrame(frame_id, page_id);
  page->is_dirty_ = false;

  return true;
}


void BufferPoolManager::FlushAllPages() {
    std::lock_guard<std::mutex> guard(latch_);

  // 迭代所有的页表
  for (auto &entry : page_table_) {
    page_id_t page_id = entry.first;
    FlushPage(page_id);
  }
}
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { 
  std::lock_guard<std::mutex> guard(latch_);

  // 查找页面是否在缓冲池里
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    // 如果在缓冲池里没找到该页面直接返回true，说明已经删除
    return true;
  }

  frame_id_t frame_id = it->second;
  Page *page = &pages_[frame_id];

  if (page->pin_count_ > 0) {
    // 不能删
    return false;
  }
  
  // 在replacer除去相应的页表
  replacer_->Remove(frame_id);
  // 删页表中的内容
  page_table_.erase(page_id);


  // 重置页，恢复没有任务占据的状态
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;

  // 将页号放到空闲页面中去
  free_list_.push_back(frame_id);


  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

// FetchPageBasic: 获取一个基本页面（无读/写锁保护），并返回 BasicPageGuard
auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { 
  // 尝试从缓冲池中获取指定 page_id 的页面
  Page *page = FetchPage(page_id);
  
  // 如果无法获取页面，则返回空的 BasicPageGuard
  if (page == nullptr) {
    return {};
  }
  
  // 返回包含页面和当前缓冲池管理器的 BasicPageGuard
  return BasicPageGuard{this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  // 尝试从缓冲池中获取指定 page_id 的页面
  Page *page = FetchPage(page_id);
  
  // 如果无法获取页面，则返回空的 ReadPageGuard
  if (page == nullptr) {
    return {};
  }

  // 读锁定页面
  page->RLatch();
  
  // 返回包含页面和当前缓冲池管理器的 ReadPageGuard
  return ReadPageGuard{this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  // 尝试从缓冲池中获取指定 page_id 的页面
  Page *page = FetchPage(page_id);
  
  // 如果无法获取页面，则返回空的 WritePageGuard
  if (page == nullptr) {
    return {};
  }

  // 写锁定页面
  page->WLatch();
  
  // 返回包含页面和当前缓冲池管理器的 WritePageGuard
  return WritePageGuard{this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {   
  // 调用 NewPage 创建一个新页面
  Page *page = NewPage(page_id);
  
  // 如果创建失败，则返回空的 BasicPageGuard
  if (page == nullptr) {
    return {};
  }

  // 返回新页面的 BasicPageGuard
  return BasicPageGuard{this, page}; 
}

auto BufferPoolManager::AllocateFrame() -> frame_id_t {
  frame_id_t frame_id = INVALID_PAGE_ID;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&frame_id)) {
    if (pages_[frame_id].IsDirty()) {
      WriteFrame(frame_id, pages_[frame_id].GetPageId());
    }
    page_table_.erase(pages_[frame_id].GetPageId());
  }

  return frame_id;
}

void BufferPoolManager::ReadFrame(frame_id_t frame_id, page_id_t page_id) {
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  auto request = DiskRequest{
      .is_write_ = false,
      .data_ = pages_[frame_id].GetData(),
      .page_id_ = page_id,
      .callback_ = std::move(promise),
  };

  disk_scheduler_->Schedule(std::move(request));
  future.get();
}

void BufferPoolManager::WriteFrame(frame_id_t frame_id, page_id_t page_id) {
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  auto request = DiskRequest{
      .is_write_ = true,
      .data_ = pages_[frame_id].GetData(),
      .page_id_ = page_id,
      .callback_ = std::move(promise),
  };

  disk_scheduler_->Schedule(std::move(request));
  future.get();
}

}  // namespace bustub
