//
// Created by Riju Mukherjee on 26-04-2025.
//
#include "../headers/ThreadPool.h"

ThreadPool* ThreadPool::instance = nullptr;

// Get the instance
ThreadPool* ThreadPool::getInstance()
{
    if (instance == nullptr)
        instance = new ThreadPool(std::thread::hardware_concurrency());
    return instance;
}
// Constructor
ThreadPool::ThreadPool(const size_t num_threads) : stop(false) {
    for (size_t i = 0; i < num_threads; ++i) {
        workers.emplace_back([this] {
            while (true) {
                std::function<void()> task;

                {   // Lock scope
                    std::unique_lock<std::mutex> lock(queue_mutex);
                    condition.wait(lock, [this] {
                        return stop || !tasks.empty();
                    });
                    if (stop && tasks.empty())
                        return;
                    task = std::move(tasks.front());
                    tasks.pop();
                }

                task();
            }
        });
    }
}

// Destructor
ThreadPool::~ThreadPool() {
    stop = true;
    condition.notify_all();
    for (std::thread &worker : workers)
        worker.join();
}
