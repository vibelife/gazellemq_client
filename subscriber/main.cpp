#include <latch>
#include <atomic>
#include "../client_lib/include/SubscriberClient.hpp"

int main() {
    using namespace std::chrono_literals;
    std::atomic<long> count{0};

    gazellemq::client::getSubscriberClient()
        .subscribe("test1", [&count](std::string&& message) {
            ++count;
            if (count == 1) {
                auto t = std::chrono::high_resolution_clock::now().time_since_epoch();
                auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t);
                printf("Current time: %zu\n", ms.count());
                count = 0;
            }
        })
        .connectToHub("ExampleSubscriber", "localhost", 5875);

    std::latch{1}.wait();
}
