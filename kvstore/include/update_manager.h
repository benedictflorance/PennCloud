#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>
using namespace std::literals::chrono_literals;

class UpdateManager
{
public:
    explicit UpdateManager() = default;

private:
    static std::atomic<int> now_;
    static std::atomic<bool> stop_;

    struct update_thread
        : private std::thread
    {
        ~update_thread();
        update_thread(update_thread&&) = default;

        using std::thread::thread;
    };

public:
    static update_thread start();
};

void update();

// source

std::atomic<int>  UpdateManager::now_{0};
std::atomic<bool> UpdateManager::stop_{false};

UpdateManager::update_thread::~update_thread()
{
    if (joinable())
    {
        stop_ = true;
        join();
    }
}

UpdateManager::update_thread
UpdateManager::start()
{
    return update_thread{[]
                         {
                             using namespace std;
                             using namespace std::chrono;
                             auto next = system_clock::now()+10s;
                             while (!stop_)
                             {
                                 update();
                                 this_thread::sleep_until(next);
                                 next = next + 10s;
                                 
                             }
                         }};
}

#include "date/date.h"

void update()
{
    using namespace date;
    using namespace std;
    using namespace std::chrono;
    cerr << system_clock::now() << '\n';
    cerr << "Checking for heartbeat"<<endl;
}

// demo

// int main()
// {
//     auto t = UpdateManager::start();
//     using namespace std;
//     this_thread::sleep_for(10s);
// }