#include <latch>
#include <atomic>
#include "../client_lib/include/SubscriberClient.hpp"
#include "Random.hpp"
#include "../common/Consts.hpp"

int main() {
    using namespace std::chrono_literals;

    std::atomic<long> count{0};

    std::string name{"ExampleSubscriber"};
    // name.append(vl::random::getString());

    gazellemq::client::getSubscriberClient()
        .subscribe("order", [&count](std::string&& message) {
            if (++count == bench::consts::NB_MESSAGES) {
                auto t = std::chrono::high_resolution_clock::now().time_since_epoch();
                auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t);
                printf("Received all %d messages: time=%zu\n", bench::consts::NB_MESSAGES, ms.count());
                count = 0;
            }
        })
        .connectToHub(name.c_str(), "localhost", 5875);

    std::latch{1}.wait();
}
