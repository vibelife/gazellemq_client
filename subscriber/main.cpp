#include <latch>
#include <atomic>
#include "../client_lib/include/SubscriberClient.hpp"
#include "Random.hpp"

int main() {
    using namespace std::chrono_literals;
    static constexpr auto NB_MESSAGES = 1;

    std::atomic<long> count{0};

    std::string name{"ExampleSubscriber_"};
    name.append(vl::random::getString());

    gazellemq::client::getSubscriberClient()
        .subscribe("test1", [&count](std::string&& message) {
            ++count;
            if (count == NB_MESSAGES) {
                auto t = std::chrono::high_resolution_clock::now().time_since_epoch();
                auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t);
                printf("Current time: %zu\n", ms.count());
                count = 0;
            }
        })
        .connectToHub(name.c_str(), "localhost", 5875);

    std::latch{1}.wait();
}
