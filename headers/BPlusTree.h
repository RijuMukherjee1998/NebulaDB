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

template<typename Key , typename Value, int Order>
class BPlusTree {
    struct Node;
    struct InternalNode;
    struct LeafNode;

    enum class NodeType { INTERNAL, LEAF };

    struct Node {
        NodeType type;
        explicit Node(NodeType t) : type(t) {}
        virtual ~Node() = default;
        std::shared_ptr<InternalNode> parent = nullptr;
        virtual size_t getKeyCount() const = 0; // polymorphic
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

    void insert(const Key& key, const Value& value)
    {
        if (!root)
        {
            auto leaf = std::make_shared<LeafNode>();
            leaf->keys.push_back(key);
            leaf->values.push_back(value);
            root = leaf;
            return;
        }

        std::shared_ptr<Node> curr = root;
        while (curr->type == NodeType::INTERNAL)
        {
            auto inode = std::static_pointer_cast<InternalNode>(curr);
            int idx = std::lower_bound(inode->keys.begin(),
                                       inode->keys.end(),
                                       key) - inode->keys.begin();
            assert(idx < (int)inode->children.size());
            curr = inode->children[idx];
        }

        auto leaf = std::static_pointer_cast<LeafNode>(curr);
        auto it = std::lower_bound(leaf->keys.begin(),
                                   leaf->keys.end(),
                                   key);
        int pos = it - leaf->keys.begin();

        leaf->keys.insert(it, key);
        leaf->values.insert(leaf->values.begin() + pos, value);

        // check if keys are sorted if not we have a bug
        assert(isSorted(leaf->keys));

        if (leaf->getKeyCount() >= Order)
            splitLeaf(leaf);
    }
    std::unique_ptr<Value> searchKey(const Key& key, bool& found)
    {
        found = false;
        std::shared_ptr<Node> curr = root;
        std::unique_ptr<Value> value = nullptr;
        if(!curr) return value;
        while (curr->type == NodeType::INTERNAL)
        {
            auto inode = std::static_pointer_cast<InternalNode>(curr);
            int idx = std::upper_bound(inode->keys.begin(),
                                       inode->keys.end(),
                                       key) - inode->keys.begin();
            assert(idx < (int)inode->children.size());
            curr = inode->children[idx];
        }
        auto leaf  = std::static_pointer_cast<LeafNode>(curr);
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
            auto inode = std::static_pointer_cast<InternalNode>(curr);
            int idx = std::lower_bound(inode->keys.begin(),
                                       inode->keys.end(),
                                       startKey) - inode->keys.begin();
            assert(idx < (int)inode->children.size());
            curr = inode->children[idx];
        }
        auto leaf = std::static_pointer_cast<LeafNode>(curr);
        while(leaf)
        {
            for(size_t i = 0; i < leaf->keys.size(); ++i)
            {
                if(leaf->keys[i] > endKey)
                {
                    return result;
                }
                if(leaf->keys[i] >= startKey && leaf->keys[i] <= endKey)
                {
                    result->emplace_back(leaf->values[i]);
                    found = true;
                }

            }
            leaf = leaf->next;
        }
        return result;
    }
    std::unique_ptr<Value> deleteKey(const Key& key, bool& found)
    {
        std::shared_ptr<Node> curr = root;
        size_t min_keys_leaf = std::ceil(Order / 2);
        if(!curr) return nullptr;
        found = false;
        std::unique_ptr<Value> deletedValue = nullptr;
        while (curr->type == NodeType::INTERNAL)
        {
            auto inode = std::static_pointer_cast<InternalNode>(curr);
            int idx = std::upper_bound(inode->keys.begin(),
                                       inode->keys.end(),
                                       key) - inode->keys.begin();
            assert(idx < (int)inode->children.size());
            curr = inode->children[idx];
        }
        auto leaf = std::static_pointer_cast<LeafNode>(curr);
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
            // handle leaf node underflow if necessary
            // For simplicity, we are not implementing rebalancing in this example
            borrow_or_merge(leaf);
        }
        return deletedValue;
    }
    std::vector<std::unique_ptr<Value>> deleteRange(const Key& startKey, const Key& endKey)
    {
        std::vector<Key> keysToDelete;
        std::vector<std::unique_ptr<Value>> deletedValues;
        std::shared_ptr<Node> curr = root;
        if(!curr) return deletedValues;
        while(curr->type == NodeType::INTERNAL)
        {
            auto inode = std::static_pointer_cast<InternalNode>(curr);
            int idx = std::lower_bound(inode->keys.begin(),
                                       inode->keys.end(),
                                       startKey) - inode->keys.begin();
            assert(idx < (int)inode->children.size());
            curr = inode->children[idx];
        }
        auto leaf = std::static_pointer_cast<LeafNode>(curr);
        bool endReached = false;
        size_t min_keys_leaf = std::ceil(Order / 2);
        while(leaf && !endReached)
        {
            for(size_t i = 0; i < leaf->keys.size(); ++i)
            {
                if(leaf->keys[i] >= startKey && leaf->keys[i] <= endKey)
                {
                    keysToDelete.push_back(leaf->keys[i]);
                    deletedValues.push_back(std::make_unique<Value>(leaf->values[i]));
                }
                if(i < leaf->keys.size() && leaf->keys[i] > endKey)
                {
                    endReached = true;
                    break;
                }
            }
            leaf = leaf->next;
        }
        int i = 0;
        bool found = false;
        while (i < keysToDelete.size())
        {
            deleteKey(keysToDelete[i], found);
            assert(found);
            ++i;
        }

        return deletedValues;
    }
    void print() const
    {
        printNode(root, 0);
    }
    bool bplustreeSortedCheck()
    {
        std::shared_ptr<Node> curr = root;
        while (curr->type != NodeType::LEAF)
        {
            auto inode = std::static_pointer_cast<InternalNode>(curr);
            // trying to the leftmost leaf value possible
            curr = inode->children[0];
        }
        auto lnode = std::static_pointer_cast<LeafNode>(curr);
        int64_t last_max_key = std::numeric_limits<int64_t>::min();
        while (lnode != nullptr)
        {
            for (int i = 1; i < lnode->keys.size(); ++i)
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
        if(node == root)
        {
            // Special case for root
            if(node->type == NodeType::INTERNAL)
            {
                auto inode = std::static_pointer_cast<InternalNode>(node);
                if(inode->getKeyCount() == 0)
                {
                    root = inode->children[0];
                    root->parent = nullptr;
                }
            }
            return;
        }
        size_t min_keys_internal = std::ceil(Order / 2) - 1;
        size_t min_keys_leaf = std::ceil(Order / 2);
        // Borrowing logic to be implemented
        if(node->type == NodeType::LEAF)
        {
            // Borrow from sibling leaf node
            auto lnode = std::static_pointer_cast<LeafNode>(node);
            if(lnode->prev && (lnode->parent == lnode->prev->parent) && lnode->prev->getKeyCount() > min_keys_leaf)
            {
                // Borrow from left sibling
                auto leftSibling = lnode->prev;
                lnode->keys.insert(lnode->keys.begin(), leftSibling->keys.back());
                lnode->values.insert(lnode->values.begin(), leftSibling->values.back());
                leftSibling->keys.pop_back();
                leftSibling->values.pop_back();
                // Update parent keys
                auto parent = lnode->parent;
                if(parent)
                {
                    int idx = 0;
                    while(idx < parent->children.size() && parent->children[idx] != lnode)
                        ++idx;
                    if(idx > 0)
                        parent->keys[idx - 1] = lnode->keys.front();
                }

            }
            else if(lnode->next && (lnode->parent == lnode->next->parent)&& lnode->next->getKeyCount() > min_keys_leaf)
            {
                // Borrow from right sibling
                auto rightSibling = lnode->next;
                lnode->keys.push_back(rightSibling->keys.front());
                lnode->values.push_back(rightSibling->values.front());
                rightSibling->keys.erase(rightSibling->keys.begin());
                rightSibling->values.erase(rightSibling->values.begin());
                // Update parent keys
                auto parent = lnode->parent;
                if(parent)
                {
                    int idx = 0;
                    while(idx < parent->children.size() && parent->children[idx] != rightSibling)
                        ++idx;
                    if(idx > 0)
                        parent->keys[idx - 1] = rightSibling->keys.front();
                }
            }
            else{
                // Need to merge
                if(lnode->prev && (lnode->parent == lnode->prev->parent))
                {
                    // Merge with left sibling
                    auto leftSibling = lnode->prev;
                    leftSibling->keys.insert(leftSibling->keys.end(),
                                             lnode->keys.begin(),
                                             lnode->keys.end());
                    leftSibling->values.insert(leftSibling->values.end(),
                                               lnode->values.begin(),
                                               lnode->values.end());
                    leftSibling->next = lnode->next;
                    if(lnode->next)
                        lnode->next->prev = leftSibling;
                    // Update parent
                    auto parent = lnode->parent;
                    int idx = 0;
                    while(idx < parent->children.size() && parent->children[idx] != lnode)
                        ++idx;
                    if(idx <= 0)
                    {
                        assert(false); // should not happen
                    }
                    parent->children.erase(parent->children.begin() + idx);
                    parent->keys.erase(parent->keys.begin() + idx - 1);
                    if(parent->getKeyCount() < min_keys_internal)
                        borrow_or_merge(parent);
                }
                else if(lnode->next && (lnode->parent == lnode->next->parent))
                {
                    // Merge with right sibling
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
                    // Update parent
                    auto parent = lnode->parent;
                    int idx = 0;
                    while(idx < parent->children.size() && parent->children[idx] != rightSibling)
                        ++idx;
                    if(idx <= 0)
                    {
                        assert(false); // should not happen
                    }
                    parent->children.erase(parent->children.begin() + idx);
                    parent->keys.erase(parent->keys.begin() + idx - 1);
                    if(parent->getKeyCount() < min_keys_internal)
                        borrow_or_merge(parent);
                }
            }
        }
        else
        {
            // Merge with parent internal node
            auto inode = std::static_pointer_cast<InternalNode>(node);
            auto parent = inode->parent;
            if(!parent) return;
            int idx = 0;
            while(idx < parent->children.size() && parent->children[idx] != inode)
                ++idx;
            if(idx > 0 && parent->children[idx - 1]->node_getKeyCount() > min_keys_internal)
            {
                // Borrow from left sibling
                auto leftSibling = std::static_pointer_cast<InternalNode>(parent->children[idx - 1]);
                auto borrowKey = leftSibling->keys.back();
                auto borrowChild = leftSibling->children.back();
                leftSibling->keys.pop_back();
                leftSibling->children.pop_back();
                inode->keys.insert(inode->keys.begin(), parent->keys[idx - 1]);
                inode->children.insert(inode->children.begin(), borrowChild);
                borrowChild->parent = inode;
                parent->keys[idx - 1] = borrowKey;

            }
            else if(idx + 1 < parent->children.size() && parent->children[idx + 1]->node_getKeyCount() > min_keys_internal)
            {
                // Borrow from right sibling
                auto rightSibling = std::static_pointer_cast<InternalNode>(parent->children[idx + 1]);
                auto borrowKey = rightSibling->keys.front();
                auto borrowChild = rightSibling->children.front();
                rightSibling->keys.erase(rightSibling->keys.begin());
                rightSibling->children.erase(rightSibling->children.begin());
                inode->keys.push_back(parent->keys[idx]);
                inode->children.push_back(borrowChild);
                borrowChild->parent = inode;
                parent->keys[idx] = borrowKey;
            }
            else{
                // Need to merge internal nodes
                if(idx > 0)
                {
                    // Merge with left sibling
                    auto leftSibling = std::static_pointer_cast<InternalNode>(parent->children[idx - 1]);
                    leftSibling->keys.push_back(parent->keys[idx - 1]);
                    leftSibling->keys.insert(leftSibling->keys.end(),
                                             inode->keys.begin(),
                                             inode->keys.end());
                    leftSibling->children.insert(leftSibling->children.end(),
                                                 inode->children.begin(),
                                                 inode->children.end());
                    for(auto& child : inode->children)
                        child->parent = leftSibling;
                    parent->children.erase(parent->children.begin() + idx);
                    parent->keys.erase(parent->keys.begin() + idx - 1);
                    if(parent->getKeyCount() < min_keys_internal)
                        borrow_or_merge(parent);
                }
                else if(idx + 1 < parent->children.size())
                {
                    // Merge with right sibling
                    auto rightSibling = std::static_pointer_cast<InternalNode>(parent->children[idx + 1]);
                    inode->keys.push_back(parent->keys[idx]);
                    inode->keys.insert(inode->keys.end(),
                                       rightSibling->keys.begin(),
                                       rightSibling->keys.end());
                    inode->children.insert(inode->children.end(),
                                           rightSibling->children.begin(),
                                           rightSibling->children.end());
                    for(auto& child : rightSibling->children)
                        child->parent = inode;
                    parent->children.erase(parent->children.begin() + idx + 1);
                    parent->keys.erase(parent->keys.begin() + idx);
                    if(parent->getKeyCount() < min_keys_internal)
                        borrow_or_merge(parent);
                }
            }

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
            return;
        }

        auto parent = lnode->parent;
        Key promoteKey = newLeaf->keys.front();

        auto it = std::lower_bound(parent->keys.begin(),
                                   parent->keys.end(),
                                   promoteKey);
        int idx = it - parent->keys.begin();

        parent->keys.insert(it, promoteKey);
        parent->children.insert(parent->children.begin() + idx + 1, newLeaf);
        assert(isSorted(parent->keys));
        newLeaf->parent = parent;

        if (parent->getKeyCount() >= Order)
            splitInternal(parent);
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
            return;
        }

        auto parent = inode->parent;
        auto it = std::lower_bound(parent->keys.begin(),
                                   parent->keys.end(),
                                   promoteKey);
        int idx = it - parent->keys.begin();

        parent->keys.insert(it, promoteKey);
        parent->children.insert(parent->children.begin() + idx + 1, newInode);
        assert(isSorted(parent->keys));
        newInode->parent = parent;

        if (parent->getKeyCount() >= Order)
            splitInternal(parent);
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