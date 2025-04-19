//
// Created by Riju Mukherjee on 01-03-2025.
//

#include "../headers/DiskManager.h"
#include <fstream>

namespace StorageEngine
{
    DiskManager::DiskManager(const std::filesystem::path& currTablePath)
    {
        this->currTablePath = currTablePath;
    }
    void DiskManager::fileWrite(const uint32_t file_id, const uint64_t pg_offset, const std::unique_ptr<char[]>& page_buffer)
    {
        const std::filesystem::path page_name = std::to_string(file_id) + ".ntb";
        const std::filesystem::path page_file_path =  currTablePath/page_name;
        std::lock_guard<std::recursive_mutex>file_lock(file_mut);
        std::ofstream outFile(page_file_path, std::ios::binary | std::ios::out);
        if (!outFile.is_open())
        {
            logger->logCritical({"Write failed ... Unable to open file"});
        }
        else
        {
            outFile.seekp(pg_offset, std::ios::beg);
            outFile.write(page_buffer.get(), PAGE_SIZE);
            outFile.close();
        }
    }

    std::shared_ptr<Page> DiskManager::fileRead(const uint32_t file_id, const uint64_t pg_offset)
    {
        const std::filesystem::path page_name = std::to_string(file_id) + ".ntb";
        const std::filesystem::path page_file_path =  currTablePath/page_name;
        std::lock_guard<std::recursive_mutex> file_lock(file_mut);
        std::ifstream inFile(page_file_path, std::ios::binary | std::ios::in);
        if (!inFile.is_open())
        {
            logger->logCritical({"Read failed ... Unable to open file"});
            return nullptr;
        }
        inFile.seekg(pg_offset);
        if (!inFile.good())
        {
            logger->logCritical({"Read failed ... Unable to seek to offset"});
            return nullptr;
        }

        const auto page_buffer = std::make_unique<char[]>(PAGE_SIZE);
        inFile.read(page_buffer.get(), PAGE_SIZE);
        if (!inFile.good())
        {
            /*This is always not an error as you have to understand that after a new PDE the page is never added to the
              buffer so the page at this offset dosen't exist have to handle it in a better way will think about it later*/
            logger->logCritical({"Read failed ... Unable to read the entire buffer" , page_file_path.string()});
            return nullptr;
        }
        return std::move(convertBufferToPage(page_buffer));
    }
    std::unique_ptr<Page> DiskManager::convertBufferToPage(const std::unique_ptr<char[]>& page_buffer)
    {
        char* buffer_ptr = page_buffer.get();
        //Deserialize PageHeader from buffer
        PageHeader header{};
        std::memcpy(&header, buffer_ptr, sizeof(PageHeader));
        buffer_ptr += sizeof(PageHeader);

        //Deserialize list of Slot from buffer
        std::list<Slot> slots(header.num_slots);
        for (auto& slot : slots)
        {
            std::memcpy(&slot, buffer_ptr, sizeof(Slot));
            buffer_ptr += sizeof(Slot);
        }
        auto page = std::make_unique<Page>();
        page->header = header;
        page->slots = std::move(slots);

        //buffer_ptr += sizeof(size_t);
        //Deserialize page_data from buffer
        const size_t pg_data_offset = sizeof(PageHeader) + header.num_slots * sizeof(Slot);
        for (size_t i = pg_data_offset; i < page->data_container_size; i++)
        {
            std::memcpy(&page->page_data[i],buffer_ptr,sizeof(char));
            buffer_ptr += sizeof(char);
        }

        return std::move(page);
    }
    // Serialize the Page to byte Buffer
    std::unique_ptr<char[]> DiskManager::convertPageToBuffer(const std::shared_ptr<Page>& pg_ptr)
    {
        std::unique_ptr<char[]> page_buffer = std::make_unique<char[]>(PAGE_SIZE);
        std::lock_guard<std::recursive_mutex> page_lock(pg_mut);
        char* buffer_ptr = page_buffer.get();

        //Copy page header to buffer
        std::memcpy(buffer_ptr, &pg_ptr->header, sizeof(PageHeader));
        buffer_ptr += sizeof(PageHeader);

        //Copy list of slots to buffer
        for (const auto& slot : pg_ptr->slots)
        {
            if (slot.isSlotValid)
            {
                std::memcpy(buffer_ptr, &slot, sizeof(Slot));
                buffer_ptr += sizeof(Slot);
            }
        }
        // Store the size of page_data
        // const size_t page_data_size = PAGE_SIZE - pg_ptr->header.num_slots * sizeof(Slot) - sizeof(PageHeader) - sizeof(size_t);
        // std::memcpy(buffer_ptr, &page_data_size, sizeof(size_t));
        // buffer_ptr += sizeof(size_t);

        const size_t pg_data_offset = sizeof(PageHeader) + pg_ptr->header.num_slots * sizeof(Slot);
        // Copy the actual page data buffer (Copy should happen from the free_page_offset to the end of the array, otherwise the buffer can cross 4KB)
        for (size_t i=pg_data_offset; i<PAGE_SIZE; i++)
        {
            std::memcpy(buffer_ptr, &pg_ptr->page_data[i], sizeof(char));
            buffer_ptr += sizeof(char);
        }
        return std::move(page_buffer);
    }
    void DiskManager::writePageToDisk(uint32_t file_id, uint64_t page_offset, const std::shared_ptr<Page>& pg_ptr)
    {
        const std::unique_ptr<char[]> page_buffer = convertPageToBuffer(pg_ptr);
        fileWrite(file_id, page_offset, page_buffer);
    }
    std::shared_ptr<Page> DiskManager::readPageFromDisk(uint32_t file_id, uint64_t page_offset)
    {
        std::shared_ptr<Page> page_from_disk = fileRead(file_id, page_offset);
        return page_from_disk;
    }
}