//
// Created by Riju Mukherjee on 02-05-2025.
//

#ifndef LRUK_H
#define LRUK_H

#include <deque>
#include <unordered_map>
#include <queue>
#include "Page.h"
#include "PageCache.h"

struct PageEntry
{
    uint64_t logical_id{};
    std::deque<uint64_t> access_times; // Stores last K access timestamps
};
class LRU_K
{
private:
    int K;  // Number of accesses to track
    std::unordered_map<uint64_t, PageEntry>* page_table = new std::unordered_map<uint64_t, PageEntry>();

    // Helper function to calculate the Kth access time or default to a large value
    uint64_t getKthAccessTime(const PageEntry& entry) const {
        if (entry.access_times.size() < K) {
            // If we don't have K accesses yet, use negative infinity (very low priority)
            return std::numeric_limits<uint64_t>::min();
        } else {
            // Use the Kth most recent access (which is the oldest in our buffer)
            return entry.access_times.back();
        }
    }

public:
    ~LRU_K() {
        delete page_table;
    }
    explicit LRU_K(const int k) : K(k) {}

    // Modified accessPage method with the correct next_access calculation
    void accessPage(uint64_t logical_id, const uint64_t timestamp) {
        // Create an entry if it doesn't exist
        if (page_table->find(logical_id) == page_table->end()) {
            (*page_table)[logical_id] = {logical_id, {}};
        }
        auto& entry = (*page_table)[logical_id];

        // Add this access to the history
        entry.access_times.push_back(timestamp);

        // Keep only the K most recent accesses
        if (entry.access_times.size() > K) {
            entry.access_times.pop_front();
        }
    }

    uint64_t evictPage(const std::shared_ptr<std::unordered_map<uint64_t, std::shared_ptr<StorageEngine::Page>>>& page_cache)
    {
        if (page_table->empty())
        {
            return UINT64_MAX;
        }
        uint64_t evict_page = UINT64_MAX;
        uint64_t oldest_timestamp = std::numeric_limits<uint64_t>::max();
        for (auto& [logical_id, entry] : *page_table)
        {
            if (getKthAccessTime(entry) < oldest_timestamp && (*page_cache)[logical_id]->pin_count == 0)
            {
                evict_page = logical_id;
                oldest_timestamp = getKthAccessTime(entry);
            }
            if (oldest_timestamp == std::numeric_limits<uint64_t>::min())
                break;
        }
        if (evict_page != UINT64_MAX)
        {
            page_table->erase(evict_page);
            return evict_page;
        }
        return UINT64_MAX; // No page to evict
    }
    // This is put in place just for testing out the evict page logic
    uint64_t evictPage()
    {
        if (page_table->empty())
        {
            return UINT64_MAX;
        }
        uint64_t evict_page = UINT64_MAX;
        uint64_t oldest_timestamp = std::numeric_limits<uint64_t>::max();
        for (auto& [logical_id, entry] : *page_table)
        {
            if (getKthAccessTime(entry) < oldest_timestamp)
            {
                evict_page = logical_id;
                oldest_timestamp = getKthAccessTime(entry);
            }
            if (oldest_timestamp == std::numeric_limits<uint64_t>::min())
                break;
        }
        if (evict_page != UINT64_MAX)
        {
            page_table->erase(evict_page);
            return evict_page;
        }
        return UINT64_MAX; // No page to evict
    }
};

#endif //LRUK_H