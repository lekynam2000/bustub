#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  std::shared_ptr<const TrieNode> curr = this->root_ ;
  for(int i=0;i<key.size();i++){
    curr = this->GetOne(curr, key[i]);
    if(curr==nullptr) return nullptr;
  }
  auto node = dynamic_cast<const TrieNodeWithValue<T> *>(curr.get());
  if(node == nullptr) return nullptr;
  return node->value_;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  std::shared_ptr<const TrieNode> newroot = std::shared_ptr<const TrieNode>(std::move(this->root->Clone())); 
  std::shared_ptr<const TrieNode> parent = nullptr, curr=newroot;
  int i=0;
  for(i=0;i<key.size();i++){
    parent = curr;
    curr = this->GetOne(parent,key[i]);
    
    if(curr==nullptr){
      curr = std::make_shared<const TrieNode>();
      parent->children_[key[i]] = curr;
    }
    else{
      parent->children_[key[i]] = std::shared_ptr<const TrieNode>(std::move(curr->Clone()));
    }
  }

  //create new curr with old curr children and parent but with new value

  auto newcurr = std::make_shared<const TrieNodeWithValue T >(curr->children_, std::move(value));
  parent->children_[key.back()] = newcurr;
  return Trie(newroot);
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) const -> Trie {
  std::shared_ptr<const TrieNode> newroot = std::shared_ptr<const TrieNode>(std::move(this->root_->Clone())); 
  std::shared_ptr<const TrieNode> parent = nullptr, curr=newroot;
  int i=0;
  for(i=0;i<key.size();i++){
    parent = curr;
    curr = this->GetOne(curr,key[i]);
    
    if(curr==nullptr) return *this;
    else{
      parent->children_[key[i]] = std::shared_ptr<const TrieNode>(std::move(curr->Clone()));
    }
  }
  auto newchild = std::make_shared<const TrieNode>(curr->children_);
  if(!newchild->children_.empty()){
    parent->children_[key.back()] = newchild;
  }
  else{
    parent->children_[key.back()] = nullptr;
  }
  return Trie(newroot);

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

auto Trie::GetOne(std::shared_ptr<const TrieNode> node, char c) const -> const std::shared_ptr<const TrieNode>{  
  std::shared_ptr<const TrieNode> newroot;
  if(node->children_.find(c)!=node->children_.end()){
    return node->children_[c];
  }
  return nullptr;
}


// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
