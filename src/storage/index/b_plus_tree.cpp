#include <stdexcept>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsSafe(BPlusTreePage *page, OpeType op) -> bool {
  if (op == OpeType::FIND) {
    return true;
  }

  if (op == OpeType::INSERT) {
    if (page->IsLeafPage()) {
      return page->GetSize() < page->GetMaxSize() - 1;
    }
    return page->GetSize() < page->GetMaxSize();
  }

  if (op == OpeType::REMOVE) {
    return page->GetSize() > page->GetMinSize();
  }

  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockPageSet(Transaction *transaction, bool is_dirty) {
  if (transaction == nullptr) {
    return;
  }
  auto deque = transaction->GetPageSet();
  while (!deque->empty()) {
    Page *page = deque->front();
    // if (page != nullptr) {
    //   LOG_DEBUG("Unlock PageId:%d", page->GetPageId());
    // }
    deque->pop_front();
    if (page == nullptr) {
      root_latch_.WUnlock();
      // LOG_INFO("ROOT_WUNLOCK");
    } else {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), is_dirty);
    }
  }
  // buffer_pool_manager_->CheckPinCount();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetPageFromTransaction(page_id_t page_id, Transaction *transaction) -> Page * {
  assert(transaction != nullptr);
  auto page_set = transaction->GetPageSet();
  for (auto it = page_set->rbegin(); it != page_set->rend(); ++it) {
    Page *page = *it;
    if (page != nullptr && page->GetPageId() == page_id) {
      return page;
    }
  }
  throw std::logic_error("Non_exist Page");
  return nullptr;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // std::scoped_lock<std::mutex> lock(latch_);

  Page *page = GetLeafPage(key, transaction, OpeType::FIND, true);
  bool found = false;
  auto leaf_page = CastLeafPage(page);
  for (int i = 0; i < leaf_page->GetSize(); ++i) {
    if (comparator_(leaf_page->KeyAt(i), key) == 0) {
      result->emplace_back(leaf_page->ValueAt(i));
      found = true;
    }
  }
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return found;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPage(const KeyType &key, Transaction *transaction, OpeType op, bool is_first) -> Page * {
  if (op == OpeType::FIND) {
    root_latch_.RLock();
  } else {
    root_latch_.WLock();
  }
  if (IsEmpty() && op == OpeType::INSERT) {
    Page *page = buffer_pool_manager_->NewPage(&root_page_id_);  //  set root_page_id_ now
    UpdateRootPageId(1);
    LeafPage *leaf_page = CastLeafPage(page);
    leaf_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    // root_latch_.WUnlock();
  }

  page_id_t cur_page_id = root_page_id_;
  Page *prev_page = nullptr;
  if (op != OpeType::FIND) {
    transaction->AddIntoPageSet(prev_page);
  }

  while (true) {
    Page *page = buffer_pool_manager_->FetchPage(cur_page_id);  // 获取当前id的page
    auto tree_page = CastBPlusPage(page);
    // LOG_DEBUG("cur_page_id:%d, is_first:%d", cur_page_id, is_first);
    if (op == OpeType::FIND) {
      page->RLatch();  // 读操作
      if (prev_page != nullptr) {
        prev_page->RUnlatch();
        buffer_pool_manager_->UnpinPage(prev_page->GetPageId(), false);
      } else {
        root_latch_.RUnlock();
      }
    } else if (!is_first) {  // 悲观写操作
      page->WLatch();
      if (IsSafe(tree_page, op)) {
        UnlockPageSet(transaction, false);
      }
      transaction->AddIntoPageSet(page);
    } else {  //  乐观写操作
      if (tree_page->IsLeafPage()) {
        // 当root 是leaf时，判断root安全插入才能释放锁
        // 否则提前释放了锁，root在这一个insert中分裂， 而别的线程依旧在这个旧的root中插入
        page->WLatch();
        transaction->AddIntoPageSet(page);
        if (prev_page != nullptr) {
          prev_page->RUnlatch();
          buffer_pool_manager_->UnpinPage(prev_page->GetPageId(), false);
        }
      } else {
        page->RLatch();
        if (prev_page != nullptr) {
          prev_page->RUnlatch();
          buffer_pool_manager_->UnpinPage(prev_page->GetPageId(), false);
        } else {
          UnlockPageSet(transaction, false);
        }
      }
    }

    if (tree_page->IsLeafPage()) {
      // LOG_DEBUG("GOT PAGE:%d", tree_page->GetPageId());
      if (!IsSafe(tree_page, op) && is_first) {
        UnlockPageSet(transaction, false);
        return GetLeafPage(key, transaction, op, false);
      }
      return page;
    }
    auto internal_page = CastInternalPage(page);  //  不是leaf page则转换成internal page
    int size = internal_page->GetSize();
    cur_page_id = internal_page->ValueAt(size - 1);
    for (int i = 1; i < size; ++i) {
      if (comparator_(internal_page->KeyAt(i), key) > 0) {
        cur_page_id = internal_page->ValueAt(i - 1);
        break;
      }
    }
    prev_page = page;
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  //  std::scoped_lock<std::mutex> lock(latch_);

  // LOG_DEBUG("INSERT %ld", key.ToString());

  Page *page = GetLeafPage(key, transaction, OpeType::INSERT, true);
  LeafPage *leaf_page = CastLeafPage(page);
  // for (int i = 0; i < leaf_page->GetSize(); ++i) {
  //   if (comparator_(key, leaf_page->KeyAt(i)) == 0) {
  //     buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  //     return false;
  //   }
  // }
  bool inserted = leaf_page->Insert(key, value, comparator_);
  //  重复键
  if (!inserted) {
    // LOG_DEBUG("DUP KEY");
    // buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    UnlockPageSet(transaction, false);
    return false;
  }
  //  空间足够
  if (leaf_page->GetSize() < leaf_max_size_) {
    // LOG_DEBUG("ENOUGH ROOM %d < %d", leaf_page->GetSize(), leaf_max_size_);
    // buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    UnlockPageSet(transaction, true);
    return true;
  }
  //  leaf page is full, split it
  // LOG_DEBUG("LEAF PAGE IS FULL, SPLIT IT %d >= %d", leaf_page->GetSize(), leaf_max_size_);
  int right_page_id;
  Page *right_page = buffer_pool_manager_->NewPage(&right_page_id);
  LeafPage *new_leaf_page = CastLeafPage(right_page);
  new_leaf_page->Init(right_page_id, leaf_page->GetParentPageId(), leaf_max_size_);
  leaf_page->Split(new_leaf_page);
  new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_leaf_page->GetPageId());

  KeyType key0 = new_leaf_page->KeyAt(0);
  // LOG_DEBUG("Key0 %ld", key0.ToString());
  InsertInParent(leaf_page, new_leaf_page, key0, transaction);

  // buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  UnlockPageSet(transaction, true);
  buffer_pool_manager_->UnpinPage(new_leaf_page->GetPageId(), true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertInParent(BPlusTreePage *left_node, BPlusTreePage *right_node, const KeyType &key0,
                                    Transaction *transaction) {
  //  left_node is root_page
  // LOG_DEBUG("InsertInParent");
  if (left_node->IsRootPage()) {
    // LOG_DEBUG("LEFT_NODE IS ROOT PAGE");
    Page *root_page = buffer_pool_manager_->NewPage(&root_page_id_);  // pin1
    InternalPage *root_node = CastInternalPage(root_page);
    root_node->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    UpdateRootPageId();
    root_node->SetKeyAt(1, key0);
    root_node->SetValueAt(0, left_node->GetPageId());
    root_node->SetValueAt(1, right_node->GetPageId());
    root_node->SetSize(2);

    left_node->SetParentPageId(root_page_id_);
    right_node->SetParentPageId(root_page_id_);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);  // unpin1
    return;
  }

  //  handle left_node's parent
  // Page *parent_page = buffer_pool_manager_->FetchPage(left_node->GetParentPageId());  // pin2 transaction
  Page *parent_page = GetPageFromTransaction(left_node->GetParentPageId(), transaction);
  InternalPage *parent_node = CastInternalPage(parent_page);
  if (parent_node->GetSize() < parent_node->GetMaxSize()) {
    parent_node->Insert(key0, right_node->GetPageId(), comparator_);
    // LOG_DEBUG("leaf page %d split parent_page id %d  ", left_node->GetPageId(), parent_node->GetPageId());
  } else {
    //  split parent node
    int right_parent_page_id;
    Page *right_parent_page = buffer_pool_manager_->NewPage(&right_parent_page_id);  // pin3
    InternalPage *right_parent_node = CastInternalPage(right_parent_page);
    right_parent_node->Init(right_parent_page_id, parent_node->GetParentPageId(), internal_max_size_);
    parent_node->Split(right_parent_node, key0, right_node->GetPageId(), comparator_);
    UpdateChildNode(right_parent_node, 0, right_parent_node->GetSize());

    KeyType parent_key0 = right_parent_node->KeyAt(0);
    // LOG_DEBUG("Parent_Key0 %ld", parent_key0.ToString());
    // right_parent_node->SetKeyAt(0, KeyType{});

    InsertInParent(parent_node, right_parent_node, parent_key0, transaction);

    buffer_pool_manager_->UnpinPage(right_parent_page_id, true);  // unpin3
  }
  // buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);  // unpin2
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateChildNode(InternalPage *node, int begin, int end) {
  for (int i = begin; i < end; i++) {
    Page *child_page = buffer_pool_manager_->FetchPage(node->ValueAt(i));
    CastBPlusPage(child_page)->SetParentPageId(node->GetPageId());
    buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // std::scoped_lock<std::mutex> lock(latch_);
  // LOG_DEBUG("LEAF %d INTERNAL %d", leaf_max_size_, internal_max_size_);
  // LOG_DEBUG("REMOVE %ld", key.ToString());

  Page *page = GetLeafPage(key, transaction, OpeType::REMOVE, true);
  LeafPage *node = CastLeafPage(page);
  bool removed = node->Remove(key, comparator_);

  if (!removed) {  //  NOT FOUND
    // buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    UnlockPageSet(transaction, false);
    return;
  }

  if (node->IsRootPage()) {
    if (node->GetSize() == 0) {  // 如果变成空树？
      root_page_id_ = INVALID_PAGE_ID;
    }
    // buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    UnlockPageSet(transaction, true);
    // LOG_DEBUG("NOT FOUND");
    return;
  }

  if (node->GetSize() >= node->GetMinSize()) {
    // LOG_INFO("SIZE>=MIN");
    // buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    UnlockPageSet(transaction, true);
    return;
  }
  // LOG_DEBUG("HAHA1");
  HandleUnderFlow(node, transaction);
  // buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  UnlockPageSet(transaction, true);

  auto deleted_pages = transaction->GetDeletedPageSet();
  for (auto &pid : *deleted_pages) {
    buffer_pool_manager_->DeletePage(pid);
  }
  deleted_pages->clear();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::HandleUnderFlow(BPlusTreePage *page, Transaction *transaction) {
  if (page->IsRootPage()) {
    if (page->IsLeafPage() || page->GetSize() > 1) {
      return;
    }
    //  如果变成长度为1的Internal_Page
    InternalPage *node = CastInternalPage(page);
    transaction->AddIntoDeletedPageSet(page->GetPageId());
    root_page_id_ = node->ValueAt(0);
    Page *new_page = buffer_pool_manager_->FetchPage(root_page_id_);
    BPlusTreePage *new_node = CastBPlusPage(new_page);
    new_node->SetParentPageId(INVALID_PAGE_ID);
    UpdateRootPageId();
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return;
  }

  Page *parent_page = GetPageFromTransaction(page->GetParentPageId(), transaction);
  InternalPage *parent_node = CastInternalPage(parent_page);

  page_id_t left_sibling_page_id;
  page_id_t right_sibling_page_id;
  GetSiblings(page, &left_sibling_page_id, &right_sibling_page_id, parent_page);
  if (left_sibling_page_id == INVALID_PAGE_ID && right_sibling_page_id == INVALID_PAGE_ID) {
    throw std::logic_error("Non-root page" + std::to_string(page->GetPageId()) + "has no sibling");
  }

  // BPlusTreePage *left_sibling_page = nullptr;
  // BPlusTreePage *right_sibling_page = nullptr;
  Page *left_sibling_page = nullptr;
  Page *right_sibling_page = nullptr;

  if (left_sibling_page_id != INVALID_PAGE_ID) {
    left_sibling_page = buffer_pool_manager_->FetchPage(left_sibling_page_id);
    left_sibling_page->WLatch();
    // left_sibling_page = CastBPlusPage(page);
  }
  if (right_sibling_page_id != INVALID_PAGE_ID) {
    right_sibling_page = buffer_pool_manager_->FetchPage(right_sibling_page_id);
    right_sibling_page->WLatch();
    // right_sibling_page = CastBPlusPage(page);
  }
  // Page *parent_page = buffer_pool_manager_->FetchPage(page->GetParentPageId());  //  transaction
  // LOG_INFO("page%d->GetParentPageId(%d)", page->GetPageId(), page->GetParentPageId());

  //  向兄弟节点借
  if (TryBorrow(page, left_sibling_page, parent_node, true) ||
      TryBorrow(page, right_sibling_page, parent_node, false)) {
    // buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    UnpinSiblingPage(left_sibling_page, right_sibling_page);
    return;
  }

  //  不够数量，向兄弟节点进行合并
  //  调整左右节点位置
  BPlusTreePage *left_page = nullptr;
  BPlusTreePage *right_page = nullptr;
  if (left_sibling_page != nullptr) {
    left_page = CastBPlusPage(left_sibling_page);
    right_page = page;
  } else {
    left_page = page;
    right_page = CastBPlusPage(right_sibling_page);
  }
  Merge(left_page, right_page, parent_node);
  transaction->AddIntoDeletedPageSet(right_page->GetPageId());
  // LOG_INFO("%d merge with %d", left_page->GetPageId(), right_page->GetPageId());
  UnpinSiblingPage(left_sibling_page, right_sibling_page);

  //  parent recur
  if (parent_node->GetSize() < parent_node->GetMinSize()) {
    HandleUnderFlow(parent_node, transaction);
  }
  // buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);  //  transaction
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::TryBorrow(BPlusTreePage *page, Page *siblingpage, InternalPage *parent_page, bool is_left)
    -> bool {
  //  兄弟节点不够数量或者为空
  if (siblingpage == nullptr) {
    return false;
  }
  BPlusTreePage *sibling_page = CastBPlusPage(siblingpage);
  if (sibling_page->GetSize() <= sibling_page->GetMinSize()) {
    // LOG_INFO("nullptr||Sibling_page_size<=MinSize");
    return false;
  }
  int parent_update_at = parent_page->FindIndex(page->GetPageId()) + (is_left ? 0 : 1);
  int sibling_borrow_at = is_left ? sibling_page->GetSize() - 1 : (page->IsLeafPage() ? 0 : 1);
  KeyType update_key;

  if (page->IsLeafPage()) {
    auto leaf_page = static_cast<LeafPage *>(page);
    auto leaf_sibling_page = static_cast<LeafPage *>(sibling_page);
    leaf_page->Insert(leaf_sibling_page->KeyAt(sibling_borrow_at), leaf_sibling_page->ValueAt(sibling_borrow_at),
                      comparator_);
    leaf_sibling_page->Remove(leaf_sibling_page->KeyAt(sibling_borrow_at), comparator_);
    update_key = is_left ? leaf_page->KeyAt(0) : leaf_sibling_page->KeyAt(0);
  } else {  // Borrow From Internal_Page
    auto internal_page = static_cast<InternalPage *>(page);
    auto internal_sibling_page = static_cast<InternalPage *>(sibling_page);
    update_key = internal_sibling_page->KeyAt(sibling_borrow_at);

    if (is_left) {
      // 如果兄弟节点在左边：父节点中该节点 value 左侧 key 插到该节点 key[1]，父节点 key 改为兄弟节点 key[size-1]，
      // 该节点 value[1] 改为原 value[0]，value[0] 改为兄弟 value[size-1]，兄弟节点 key,value[size-1] 删除

      internal_page->ShiftRight();
      internal_page->SetKeyAt(1, parent_page->KeyAt(parent_update_at));
      internal_page->SetValueAt(0, internal_sibling_page->ValueAt(internal_sibling_page->GetSize() - 1));
      internal_sibling_page->IncreaseSize(-1);

      UpdateChildNode(internal_page, 0, 1);

    } else {
      // 如果兄弟节点在右边：父节点中该节点 value 右侧 key 搭配兄弟节点 value[0] 插到该节点最右边，
      // 父节点 key 改为兄弟节点 key[1]，兄弟节点 value[0] 和 key[1] 删除
      internal_page->SetKeyAt(internal_page->GetSize(), parent_page->KeyAt(parent_update_at));
      internal_page->SetValueAt(internal_page->GetSize(), internal_sibling_page->ValueAt(0));
      internal_page->IncreaseSize(1);

      internal_sibling_page->ShiftLeft();

      UpdateChildNode(internal_page, internal_page->GetSize() - 1, internal_page->GetSize());
    }
  }

  parent_page->SetKeyAt(parent_update_at, update_key);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::GetSiblings(BPlusTreePage *page, page_id_t *left_page_id, page_id_t *right_page_id,
                                 Page *parent_page) {
  if (page->IsRootPage()) {
    throw std::invalid_argument("Cannot get the sibling of root page");
  }

  // Page *parent_page = buffer_pool_manager_->FetchPage(page->GetParentPageId());  // transaction
  // LOG_INFO("page%d->GetParentPageId(%d)", page->GetPageId(), page->GetParentPageId());
  InternalPage *parent_node = CastInternalPage(parent_page);
  int index = parent_node->FindIndex(page->GetPageId());
  if (index == -1) {
    throw std::logic_error("Cannot index of parent_node");
  }
  *left_page_id = *right_page_id = INVALID_PAGE_ID;
  if (index != 0) {
    *left_page_id = parent_node->ValueAt(index - 1);
  }
  if (index != parent_node->GetSize() - 1) {
    *right_page_id = parent_node->ValueAt(index + 1);
  }
  // buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);  // transaction
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Merge(BPlusTreePage *left_page, BPlusTreePage *right_page, InternalPage *parent_page) {
  int position_left = parent_page->FindIndex(left_page->GetPageId());

  if (left_page->IsLeafPage()) {
    LeafPage *left_node = CastLeafPage(left_page);
    LeafPage *right_node = CastLeafPage(right_page);

    for (int i = 0; i < right_node->GetSize(); ++i) {
      left_node->Insert(right_node->KeyAt(i), right_node->ValueAt(i), comparator_);
    }
    left_node->SetNextPageId(right_node->GetNextPageId());
  } else {
    InternalPage *left_node = CastInternalPage(left_page);
    InternalPage *right_node = CastInternalPage(right_page);
    int old_size = left_node->GetSize();

    left_node->SetKeyAt(left_node->GetSize(), parent_page->KeyAt(position_left + 1));
    left_node->SetValueAt(left_node->GetSize(), right_node->ValueAt(0));
    left_node->IncreaseSize(1);

    for (int i = 1; i < right_node->GetSize(); ++i) {
      left_node->Insert(right_node->KeyAt(i), right_node->ValueAt(i), comparator_);
    }
    UpdateChildNode(left_node, old_size, left_node->GetSize());
  }

  parent_page->ShiftLeft(position_left + 1);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnpinSiblingPage(Page *left_page, Page *right_page) {
  if (left_page != nullptr) {
    left_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(left_page->GetPageId(), true);
  }
  if (right_page != nullptr) {
    right_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(right_page->GetPageId(), true);
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  // std::scoped_lock<std::mutex> lock(latch_);
  // LOG_INFO("BEGIN");
  root_latch_.RLock();
  page_id_t cur_page_id = root_page_id_;
  Page *prev_page = nullptr;
  while (true) {
    Page *page = buffer_pool_manager_->FetchPage(cur_page_id);
    auto tree_node = CastBPlusPage(page);

    page->RLatch();
    if (prev_page == nullptr) {
      root_latch_.RUnlock();
    } else {
      prev_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(prev_page->GetPageId(), false);
    }

    if (tree_node->IsLeafPage()) {
      INDEXITERATOR_TYPE iterator(page->GetPageId(), 0, buffer_pool_manager_, CastLeafPage(page)->KeyValueAt(0));
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      return iterator;
    }
    auto internal_node = CastInternalPage(page);
    cur_page_id = internal_node->ValueAt(0);
    // buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    prev_page = page;
  }
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  // std::scoped_lock<std::mutex> lock(latch_);
  // LOG_INFO("BEGIN KEY");
  Page *page = GetLeafPage(key, nullptr, OpeType::FIND, true);
  auto node = CastLeafPage(page);
  int position = node->LowerBound(key, comparator_);
  if (position == node->GetSize() || comparator_(node->KeyAt(position), key) != 0) {
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return {};
  }
  INDEXITERATOR_TYPE iterator(node->GetPageId(), position, buffer_pool_manager_, node->KeyValueAt(position));
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return iterator;
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return {}; }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
