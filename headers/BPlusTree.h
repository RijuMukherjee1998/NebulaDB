//
// Created by Riju Mukherjee on 30-12-2025.
//

#ifndef BPLUSTREE_H
#define BPLUSTREE_H

#include <iostream>
#include <vector>
#include <memory>
#include <algorithm>
#include <cassert>
#include <cmath>
#include <shared_mutex>
#include <utility>

template<typename Key , typename Value, int Order>
class BPlusTree {
    struct Node;
    struct InternalNode;
    struct LeafNode;

    enum class NodeType { INTERNAL, LEAF };

    struct Node {
        NodeType type;
        mutable std::shared_mutex rw_mtx;
        explicit Node(NodeType t) : type(t) {}
        virtual ~Node() = default;
        std::shared_ptr<InternalNode> parent = nullptr;
        virtual size_t getKeyCount() const = 0;
        size_t node_getKeyCount() const { return getKeyCount(); }

    };

    struct InternalNode : Node {
        std::vector<Key> keys;
        std::vector<std::shared_ptr<Node>> children;
        InternalNode() : Node(NodeType::INTERNAL) {}
        size_t getKeyCount() const override{ return keys.size(); }
    };

    struct LeafNode : Node {
        std::vector<Key> keys;
        std::vector<Value> values;
        std::shared_ptr<LeafNode> next = nullptr;
        std::shared_ptr<LeafNode> prev = nullptr;
        LeafNode() : Node(NodeType::LEAF) {}
        size_t getKeyCount() const override{ return keys.size(); }
    };

    std::shared_ptr<Node> root = nullptr;

public:
    BPlusTree() = default;
    std::unique_ptr<std::vector<std::pair<Key,Value>>> getAllIndices()
    {
        std::shared_ptr<Node> curr = root;
        std::unique_ptr<std::vector<std::pair<Key,Value>>> all_indices = std::make_unique<std::vector<std::pair<Key, Value>>>();
        if (curr == nullptr)
            return all_indices;
        while (curr != nullptr && curr->type == NodeType::INTERNAL) {
            std::shared_lock<std::shared_mutex> r_lock(curr->rw_mtx);
            auto inode = std::static_pointer_cast<InternalNode>(curr);
            int idx = 0;
            assert(inode->getKeyCount() > 0);
            curr = inode->children[idx];
        }
        auto leaf = std::static_pointer_cast<LeafNode>(curr);
        while (leaf != nullptr) {
            std::shared_lock<std::shared_mutex> r_lock(leaf->rw_mtx);
            for (size_t i=0; i<leaf->getKeyCount(); i++) {
                all_indices->emplace_back(std::pair<Key,Value>(leaf->keys[i],leaf->values[i]));
            }
            leaf = leaf->next;
        }
        return all_indices;
    }

    void insert(const Key& key, const Value& value)
    {
        if (!root)
        {
            auto leaf = std::make_shared<LeafNode>();
            std::unique_lock<std::shared_mutex> w_lock(leaf->rw_mtx);
            leaf->keys.push_back(key);
            leaf->values.push_back(value);
            root = leaf;
            return;
        }

        /*
         * We are following a system where we don't want to take
         * proactive write locks on the parent nodes which again needs to be unlocked
         * by traversing up the tree if split is not happening.
         * If we want to reduce the no of times we split we can increase
         * the order of our nodes which will ensure that we split less
         * and we keep maintaining fewer write locks on the parent and leaf.
         * So while traversing down the tree we can take easy read locks and while getting to the
         * actual leaf we can take a write lock there. This will ensure that the internal nodes are
         * mostly free for reads and starvation is much lesser .
         */
        std::shared_ptr<Node> curr = root;
        while (curr->type == NodeType::INTERNAL)
        {
            std::shared_lock<std::shared_mutex> r_lock(curr->rw_mtx);
            auto inode = std::static_pointer_cast<InternalNode>(curr);

            int idx = std::lower_bound(inode->keys.begin(),
                                       inode->keys.end(),
                                       key) - inode->keys.begin();

            /* check if the idx in lower_bound is within range */
            assert(idx < (int)inode->children.size());
            curr = inode->children[idx];
        }

        auto leaf = std::static_pointer_cast<LeafNode>(curr);
        /*
         * Not using RAII for this lock because the leaf can be used in split leaf
         * so in there we have to unlock that leaf after use and we dont want to wait till
         * the lock goes out of scope and unlocks.
         */
        leaf->rw_mtx.lock();
        auto it = std::lower_bound(leaf->keys.begin(),
                                   leaf->keys.end(),
                                   key);
        int pos = it - leaf->keys.begin();

        leaf->keys.insert(it, key);
        leaf->values.insert(leaf->values.begin() + pos, value);

        /* check if keys are sorted if not we have a bug */
        assert(isSorted(leaf->keys));

        if (leaf->getKeyCount() >= Order) {
            splitLeaf(leaf);
            return;
        }
        leaf->rw_mtx.unlock();
    }
    std::unique_ptr<Value> searchKey(const Key& key, bool& found)
    {
        found = false;
        std::shared_ptr<Node> curr = root;
        std::unique_ptr<Value> value = nullptr;
        if(!curr) return value;
        while (curr->type == NodeType::INTERNAL)
        {
            std::shared_lock<std::shared_mutex> r_lock(curr->rw_mtx);
            auto inode = std::static_pointer_cast<InternalNode>(curr);
            int idx = std::upper_bound(inode->keys.begin(),
                                       inode->keys.end(),
                                       key) - inode->keys.begin();
            assert(idx < (int)inode->children.size());
            curr = inode->children[idx];
        }
        auto leaf  = std::static_pointer_cast<LeafNode>(curr);
        std::shared_lock<std::shared_mutex> r_lock(leaf->rw_mtx);

        auto it = std::lower_bound(leaf->keys.begin(),
                                   leaf->keys.end(),
                                   key);
        int pos = it - leaf->keys.begin();
        if (it != leaf->keys.end() && *it == key)
        {
            value = std::make_unique<Value>(leaf->values[pos]);
            found = true;
        }
        return value;
    }
    std::unique_ptr<std::vector<Value>> searchRange(const Key& startKey, const Key& endKey, bool& found)
    {
        found = false;
        auto result = std::make_unique<std::vector<Value>>();
        std::shared_ptr<Node> curr = root;
        if(!root || startKey > endKey)
            return result;
        while(curr->type == NodeType::INTERNAL)
        {
            std::shared_lock<std::shared_mutex> r_lock(curr->rw_mtx);
            auto inode = std::static_pointer_cast<InternalNode>(curr);
            int idx = std::lower_bound(inode->keys.begin(),
                                       inode->keys.end(),
                                       startKey) - inode->keys.begin();
            assert(idx < (int)inode->children.size());
            curr = inode->children[idx];
        }
        auto leaf = std::static_pointer_cast<LeafNode>(curr);
        if (leaf) {
            leaf->rw_mtx.lock_shared();
        }
        while(leaf) {
            for(size_t i = 0; i < leaf->keys.size(); ++i)
            {
                if(leaf->keys[i] > endKey)
                {
                    leaf->rw_mtx.unlock_shared();
                    return result;
                }
                if(leaf->keys[i] >= startKey && leaf->keys[i] <= endKey)
                {
                    result->emplace_back(leaf->values[i]);
                    found = true;
                }
            }
            auto old_leaf = leaf;
            leaf = leaf->next;
            if (leaf) {
                leaf->rw_mtx.lock_shared();
            }
            old_leaf->rw_mtx.unlock_shared();
        }
        return result;
    }
    std::unique_ptr<Value> deleteKey(const Key& key, bool& found)
    {
        std::shared_ptr<Node> curr = root;
        curr->rw_mtx.lock_shared();
        size_t min_keys_leaf = std::ceil(Order / 2);
        if(!curr) return nullptr;
        found = false;
        std::unique_ptr<Value> deletedValue = nullptr;
        while (curr->type == NodeType::INTERNAL)
        {
            auto inode = std::static_pointer_cast<InternalNode>(curr);
            std::shared_lock<std::shared_mutex> r_lock(inode->rw_mtx);
            int idx = std::upper_bound(inode->keys.begin(),
                                       inode->keys.end(),
                                       key) - inode->keys.begin();
            assert(idx < (int)inode->children.size());
            curr = inode->children[idx];
        }
        auto leaf = std::static_pointer_cast<LeafNode>(curr);
        leaf->rw_mtx.lock();
        auto it = std::lower_bound(leaf->keys.begin(),
                                   leaf->keys.end(),
                                   key);
        int pos = it - leaf->keys.begin();
        if (it != leaf->keys.end() && *it == key)
        {
            deletedValue = std::make_unique<Value>(leaf->values[pos]);
            leaf->keys.erase(it);
            leaf->values.erase(leaf->values.begin() + pos);
            found = true;
        }
        if(leaf != root && leaf->getKeyCount() < min_keys_leaf)
        {
            /* handle leaf node underflow if necessary for simplicity,
             * we are not implementing rebalancing in this example
             */
            borrow_or_merge(leaf);
            return deletedValue;
        }
        leaf->rw_mtx.unlock();
        return deletedValue;
    }
    std::vector<std::pair<Key, Value>> deleteRange(const Key& startKey, const Key& endKey)
    {
        std::vector<Key> keysToDelete;
        std::vector<std::pair<Key, Value>> deletedValues;
        std::shared_ptr<Node> curr = root;
        if(!curr) return deletedValues;
        while(curr->type == NodeType::INTERNAL)
        {
            auto inode = std::static_pointer_cast<InternalNode>(curr);
            std::shared_lock<std::shared_mutex> r_lock(inode->rw_mtx);
            int idx = std::lower_bound(inode->keys.begin(),
                                       inode->keys.end(),
                                       startKey) - inode->keys.begin();
            assert(idx < (int)inode->children.size());
            curr = inode->children[idx];
        }
        auto leaf = std::static_pointer_cast<LeafNode>(curr);
        bool endReached = false;
        while(leaf && !endReached)
        {
            for(size_t i = 0; i < leaf->keys.size(); ++i)
            {
                if(leaf->keys[i] >= startKey && leaf->keys[i] <= endKey)
                {
                    keysToDelete.push_back(leaf->keys[i]);
                    deletedValues.emplace_back(leaf->keys[i], leaf->values[i]);
                }
                if(i < leaf->keys.size() && leaf->keys[i] > endKey)
                {
                    endReached = true;
                    break;
                }
            }
            leaf = leaf->next;
        }
        size_t i = 0;
        bool found = false;
        while (i < keysToDelete.size())
        {
            deleteKey(keysToDelete[i], found);
            assert(found);
            ++i;
        }

        return deletedValues;
    }
    /* Deletes a single entry */
    void deleteEntry(const std::pair<Key, Value>& entry, bool& found) {
        found = false;
        if (!root) return;

        const Key& key = entry.first;
        const Value& val = entry.second;

        std::shared_ptr<Node> curr = root;
        while (curr->type == NodeType::INTERNAL) {
            auto inode = std::static_pointer_cast<InternalNode>(curr);
            std::shared_lock<std::shared_mutex> r_lock(inode->rw_mtx);
            int idx = std::lower_bound(inode->keys.begin(), inode->keys.end(), key) - inode->keys.begin();
            curr = inode->children[idx];
        }

        auto leaf = std::static_pointer_cast<LeafNode>(curr);
        if (leaf)
            leaf->rw_mtx.lock_shared();
        while (leaf->prev && !leaf->prev->keys.empty() && leaf->prev->keys.back() >= key) {
            auto leaf_old = leaf;
            leaf = leaf->prev;
            leaf->rw_mtx.lock_shared();
            leaf_old->rw_mtx.unlock_shared();
        }
        if (leaf) {
            leaf->rw_mtx.unlock_shared();
            leaf->rw_mtx.lock();
        }
        while (leaf) {
            for (size_t pos = 0; pos < leaf->keys.size(); ++pos) {
                if (leaf->keys[pos] > key) {
                    leaf->rw_mtx.unlock();
                    return;
                }

                if (leaf->keys[pos] == key && leaf->values[pos] == val) {
                    leaf->keys.erase(leaf->keys.begin() + pos);
                    leaf->values.erase(leaf->values.begin() + pos);
                    found = true;

                    if (leaf != root && leaf->getKeyCount() < std::ceil(Order / 2)) {
                        leaf->rw_mtx.unlock();
                        borrow_or_merge(leaf);
                        return;
                    }
                    leaf->rw_mtx.unlock();
                    return;
                }
            }
            auto leaf_old = leaf;
            leaf = leaf->next;
            if (leaf) {
                leaf->rw_mtx.lock();
            }
            leaf_old->rw_mtx.unlock();
        }
    }
    /* This exactly deletes what is needed both key and values match */
    void deleteRangeEntries(std::vector<std::pair<Key ,Value>>* del_entries, size_t& changedRows) {
        for (auto& entry : *del_entries) {
            bool found = false;
            deleteEntry(entry,found);
            //assert(found);
            if (found)
                changedRows++;
        }
    }
    void print() const
    {
        printNode(root, 0);
    }
    bool bplustreeSortedCheck()
    {
        std::shared_ptr<Node> curr = root;
        curr->rw_mtx.lock_shared();
        while (curr->type != NodeType::LEAF)
        {
            auto inode = std::static_pointer_cast<InternalNode>(curr);
            /* trying to get to the leftmost leaf value possible */
            auto curr_old = curr;
            curr = inode->children[0];
            curr->rw_mtx.lock_shared();
            curr_old->rw_mtx.unlock_shared();
        }
        auto lnode = std::static_pointer_cast<LeafNode>(curr);
        int64_t last_max_key = std::numeric_limits<int64_t>::min();
        while (lnode != nullptr)
        {
            for (size_t i = 1; i < lnode->keys.size(); ++i)
            {
                if (lnode->keys[i] < lnode->keys[i - 1] || lnode->keys[i] < last_max_key)
                {
                    std::cout << "B+Tree is not sorted in region " << lnode->keys[i-1] << "--" <<lnode->keys[i] << "--" << last_max_key <<std::endl;
                    return false;
                }
            }
            last_max_key = lnode->keys[lnode->keys.size() - 1];
            lnode = lnode->next;
        }
        return true;
    }

private:
    void borrow_or_merge(std::shared_ptr<Node> node)
    {
        if(!node) return;
        node->rw_mtx.lock();
        if(node == root)
        {
            /* Special case for root */
            if(node->type == NodeType::INTERNAL)
            {
                auto inode = std::static_pointer_cast<InternalNode>(node);
                if(inode->getKeyCount() == 0)
                {
                    root = inode->children[0];
                    root->parent = nullptr;
                }
            }
            node->rw_mtx.unlock();
            return;
        }
        size_t min_keys_internal = std::ceil(Order / 2) - 1;
        size_t min_keys_leaf = std::ceil(Order / 2);
        /* Borrowing logic to be implemented */
        if(node->type == NodeType::LEAF)
        {
            /* Borrow from the sibling leaf node */
            auto lnode = std::static_pointer_cast<LeafNode>(node);
            if (lnode->parent)
                lnode->parent->rw_mtx.lock();
            if (lnode->prev)
                lnode->prev->rw_mtx.lock();
            if (lnode->next)
                lnode->next->rw_mtx.lock();

            if(lnode->prev  && (lnode->parent == lnode->prev->parent) && (lnode->prev->getKeyCount() > min_keys_leaf))
            {
                /*immediately release the right sibling as u have nothing to do with it*/
                if (lnode->next)
                    lnode->next->rw_mtx.unlock();
                /* Borrow from left sibling */
                auto leftSibling = lnode->prev;
                lnode->keys.insert(lnode->keys.begin(), leftSibling->keys.back());
                lnode->values.insert(lnode->values.begin(), leftSibling->values.back());
                leftSibling->keys.pop_back();
                leftSibling->values.pop_back();
                leftSibling->rw_mtx.unlock();

                /* Update parent keys */
                auto parent = lnode->parent;
                if(parent)
                {
                    size_t idx = 0;
                    while(idx < parent->children.size() && parent->children[idx] != lnode)
                        ++idx;
                    if(idx > 0)
                        parent->keys[idx - 1] = lnode->keys.front();
                }
                parent->rw_mtx.unlock();
                lnode->rw_mtx.unlock();
                return;
            }
            else if(lnode->next  && (lnode->parent == lnode->next->parent) && lnode->next->getKeyCount() > min_keys_leaf)
            {
                /* immediately release the left sibling as u have nothing to do with it*/
                if (lnode->prev)
                    lnode->prev->rw_mtx.unlock();
                /* Borrow from right sibling */
                auto rightSibling = lnode->next;
                lnode->keys.push_back(rightSibling->keys.front());
                lnode->values.push_back(rightSibling->values.front());
                rightSibling->keys.erase(rightSibling->keys.begin());
                rightSibling->values.erase(rightSibling->values.begin());
                rightSibling->rw_mtx.unlock();

                /* Update parent keys */
                auto parent = lnode->parent;
                if(parent)
                {
                    size_t idx = 0;
                    while(idx < parent->children.size() && parent->children[idx] != rightSibling)
                        ++idx;
                    if(idx > 0)
                        parent->keys[idx - 1] = rightSibling->keys.front();
                }
                parent->rw_mtx.unlock();
                lnode->rw_mtx.unlock();
                return;
            }
            else {
                /* Need to merge */
                if(lnode->prev && (lnode->parent == lnode->prev->parent))
                {
                    /* Merge with left sibling */
                    auto leftSibling = lnode->prev;
                    leftSibling->keys.insert(leftSibling->keys.end(),
                                             lnode->keys.begin(),
                                             lnode->keys.end());
                    leftSibling->values.insert(leftSibling->values.end(),
                                               lnode->values.begin(),
                                               lnode->values.end());
                    leftSibling->next = lnode->next;
                    if(lnode->next) {
                        lnode->next->prev = leftSibling;
                        lnode->next->rw_mtx.unlock();
                    }
                    lnode->prev->rw_mtx.unlock();

                    /* Update parent */
                    auto parent = lnode->parent;
                    size_t idx = 0;
                    while(idx < parent->children.size() && parent->children[idx] != lnode)
                        ++idx;
                    if(idx <= 0)
                    {
                        assert(false); // should not happen
                    }
                    parent->children.erase(parent->children.begin() + idx);
                    parent->keys.erase(parent->keys.begin() + idx - 1);
                    if(parent->getKeyCount() < min_keys_internal) {
                        parent->rw_mtx.unlock();
                        borrow_or_merge(parent);
                        return;
                    }
                    parent->rw_mtx.unlock();
                    return;
                }
                if(lnode->next && (lnode->parent == lnode->next->parent))
                {
                    /*immediately release the left sibling nothing to do with it*/
                    if (lnode->prev)
                        lnode->prev->rw_mtx.unlock();
                    /* Merge with right sibling */
                    auto rightSibling = lnode->next;
                    lnode->keys.insert(lnode->keys.end(),
                                       rightSibling->keys.begin(),
                                       rightSibling->keys.end());
                    lnode->values.insert(lnode->values.end(),
                                         rightSibling->values.begin(),
                                         rightSibling->values.end());
                    lnode->next = rightSibling->next;
                    if(rightSibling->next)
                        rightSibling->next->prev = lnode;

                    rightSibling->rw_mtx.unlock();
                    lnode->rw_mtx.unlock();

                    /* Update parent */
                    auto parent = lnode->parent;
                    size_t idx = 0;
                    while(idx < parent->children.size() && parent->children[idx] != rightSibling)
                        ++idx;
                    if(idx <= 0)
                    {
                        assert(false); // should not happen
                    }
                    parent->children.erase(parent->children.begin() + idx);
                    parent->keys.erase(parent->keys.begin() + idx - 1);
                    if(parent->getKeyCount() < min_keys_internal) {
                        parent->rw_mtx.unlock();
                        borrow_or_merge(parent);
                        return;
                    }
                    return;
                }
            }
            lnode->rw_mtx.unlock();
            lnode->parent->rw_mtx.unlock();
            lnode->prev->rw_mtx.unlock();
            lnode->next->rw_mtx.unlock();
        }
        else
        {
            /* Merge with parent internal node */
            auto inode = std::static_pointer_cast<InternalNode>(node);
            auto parent = inode->parent;

            if(!parent) return;
            parent->rw_mtx.lock();

            size_t idx = 0;
            while(idx < parent->children.size() && parent->children[idx] != inode) {
                ++idx;
            }
            if(idx > 0 && parent->children[idx - 1]->node_getKeyCount() > min_keys_internal)
            {
                /* Borrow from left sibling */
                auto leftSibling = std::static_pointer_cast<InternalNode>(parent->children[idx - 1]);
                leftSibling->rw_mtx.lock();

                auto borrowKey = leftSibling->keys.back();
                auto borrowChild = leftSibling->children.back();
                borrowChild->rw_mtx.lock();

                leftSibling->keys.pop_back();
                leftSibling->children.pop_back();
                inode->keys.insert(inode->keys.begin(), parent->keys[idx - 1]);
                inode->children.insert(inode->children.begin(), borrowChild);
                borrowChild->parent = inode;
                parent->keys[idx - 1] = borrowKey;

                borrowChild->rw_mtx.unlock();
                leftSibling->rw_mtx.unlock();
                inode->rw_mtx.unlock();
                parent->rw_mtx.unlock();
                return;
            }
            else if(idx + 1 < parent->children.size() && parent->children[idx + 1]->node_getKeyCount() > min_keys_internal)
            {
                /* Borrow from right sibling */
                auto rightSibling = std::static_pointer_cast<InternalNode>(parent->children[idx + 1]);
                rightSibling->rw_mtx.lock();

                auto borrowKey = rightSibling->keys.front();
                auto borrowChild = rightSibling->children.front();
                borrowChild->rw_mtx.lock();

                rightSibling->keys.erase(rightSibling->keys.begin());
                rightSibling->children.erase(rightSibling->children.begin());
                inode->keys.push_back(parent->keys[idx]);
                inode->children.push_back(borrowChild);
                borrowChild->parent = inode;
                parent->keys[idx] = borrowKey;

                borrowChild->rw_mtx.unlock();
                rightSibling->rw_mtx.unlock();
                inode->rw_mtx.unlock();
                parent->rw_mtx.unlock();
                return;
            }
            else{
                /* Need to merge internal nodes */
                if(idx > 0)
                {
                    /* Merge with left sibling */
                    auto leftSibling = std::static_pointer_cast<InternalNode>(parent->children[idx - 1]);
                    leftSibling->rw_mtx.lock();

                    leftSibling->keys.push_back(parent->keys[idx - 1]);
                    leftSibling->keys.insert(leftSibling->keys.end(),
                                             inode->keys.begin(),
                                             inode->keys.end());
                    leftSibling->children.insert(leftSibling->children.end(),
                                                 inode->children.begin(),
                                                 inode->children.end());
                    for(auto& child : inode->children) {
                        child->rw_mtx.lock();
                        child->parent = leftSibling;
                        child->rw_mtx.unlock();
                    }
                    leftSibling->rw_mtx.unlock();
                    inode->rw_mtx.unlock();

                    parent->children.erase(parent->children.begin() + idx);
                    parent->keys.erase(parent->keys.begin() + idx - 1);
                    if(parent->getKeyCount() < min_keys_internal) {
                        parent->rw_mtx.unlock();
                        borrow_or_merge(parent);
                        return;
                    }
                    parent->rw_mtx.unlock();
                    return;
                }
                else if(idx + 1 < parent->children.size())
                {
                    /* Merge with right sibling */
                    auto rightSibling = std::static_pointer_cast<InternalNode>(parent->children[idx + 1]);
                    rightSibling->rw_mtx.lock();
                    inode->keys.push_back(parent->keys[idx]);
                    inode->keys.insert(inode->keys.end(),
                                       rightSibling->keys.begin(),
                                       rightSibling->keys.end());
                    inode->children.insert(inode->children.end(),
                                           rightSibling->children.begin(),
                                           rightSibling->children.end());
                    for(auto& child : rightSibling->children) {
                        child->rw_mtx.lock();
                        child->parent = inode;
                        child->rw_mtx.unlock();
                    }
                    rightSibling->rw_mtx.unlock();
                    inode->rw_mtx.unlock();

                    parent->children.erase(parent->children.begin() + idx + 1);
                    parent->keys.erase(parent->keys.begin() + idx);
                    if(parent->getKeyCount() < min_keys_internal) {
                        parent->rw_mtx.unlock();
                        borrow_or_merge(parent);
                        return;
                    }
                    parent->rw_mtx.unlock();
                    return;
                }
            }
            inode->rw_mtx.unlock();
            parent->rw_mtx.unlock();
        }
    }
    void splitLeaf(std::shared_ptr<LeafNode> lnode)
    {
        auto newLeaf = std::make_shared<LeafNode>();
        int mid = lnode->getKeyCount() / 2;

        newLeaf->keys.assign(lnode->keys.begin() + mid, lnode->keys.end());
        newLeaf->values.assign(lnode->values.begin() + mid, lnode->values.end());

        lnode->keys.resize(mid);
        lnode->values.resize(mid);

        newLeaf->next = lnode->next;
        if (newLeaf->next) newLeaf->next->prev = newLeaf;
        lnode->next = newLeaf;
        newLeaf->prev = lnode;

        if (!lnode->parent)
        {
            auto newRoot = std::make_shared<InternalNode>();
            newRoot->keys.push_back(newLeaf->keys.front());
            newRoot->children = { lnode, newLeaf };
            lnode->parent = newRoot;
            newLeaf->parent = newRoot;
            root = newRoot;
            lnode->rw_mtx.unlock();
            return;
        }
        auto parent = lnode->parent;
        parent->rw_mtx.lock();
        lnode->rw_mtx.unlock();

        Key promoteKey = newLeaf->keys.front();

        auto it = std::lower_bound(parent->keys.begin(),
                                   parent->keys.end(),
                                   promoteKey);
        int idx = it - parent->keys.begin();

        parent->keys.insert(it, promoteKey);
        parent->children.insert(parent->children.begin() + idx + 1, newLeaf);
        assert(isSorted(parent->keys));
        newLeaf->parent = parent;

        if (parent->getKeyCount() >= Order) {
            splitInternal(parent);
            return;
        }
        parent->rw_mtx.unlock();
    }

    void splitInternal(std::shared_ptr<InternalNode> inode)
    {
        auto newInode = std::make_shared<InternalNode>();
        int mid = inode->getKeyCount() / 2;
        Key promoteKey = inode->keys[mid];

        newInode->keys.assign(inode->keys.begin() + mid + 1, inode->keys.end());
        newInode->children.assign(inode->children.begin() + mid + 1,
                                  inode->children.end());

        for (auto& c : newInode->children)
            c->parent = newInode;

        inode->keys.resize(mid);
        inode->children.resize(mid + 1);

        if (!inode->parent)
        {
            auto newRoot = std::make_shared<InternalNode>();
            newRoot->keys.push_back(promoteKey);
            newRoot->children = { inode, newInode };
            inode->parent = newRoot;
            newInode->parent = newRoot;
            root = newRoot;
            inode->rw_mtx.unlock();
            return;
        }

        auto parent = inode->parent;
        parent->rw_mtx.lock();
        inode->rw_mtx.unlock();

        auto it = std::lower_bound(parent->keys.begin(),
                                   parent->keys.end(),
                                   promoteKey);
        int idx = it - parent->keys.begin();

        parent->keys.insert(it, promoteKey);
        parent->children.insert(parent->children.begin() + idx + 1, newInode);
        assert(isSorted(parent->keys));
        newInode->parent = parent;

        if (parent->getKeyCount() >= Order) {
            splitInternal(parent);
            return;
        }
        parent->rw_mtx.unlock();
    }

    void printNode(std::shared_ptr<Node> node, int level) const
    {
        if (!node) return;

        if (node->type == NodeType::LEAF)
        {
            auto leaf = std::static_pointer_cast<LeafNode>(node);
            std::cout << std::string(level, ' ') << "Leaf: ";
            for (auto& k : leaf->keys) std::cout << k << " ";
            std::cout << "\n";
        }
        else
        {
            auto in = std::static_pointer_cast<InternalNode>(node);
            std::cout << std::string(level, ' ') << "Internal: ";
            for (auto& k : in->keys) std::cout << k << " ";
            std::cout << "\n";
            for (auto& c : in->children)
                printNode(c, level + 2);
        }
    }


    static inline bool isSorted(const std::vector<Key>& keys)
    {
        return std::is_sorted(keys.begin(), keys.end());
    }
};




#endif //BPLUSTREE_H