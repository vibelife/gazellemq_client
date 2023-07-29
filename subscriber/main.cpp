#include <latch>
#include <thread>
#include "../client_lib/include/SubscriberClient.hpp"

int main() {
    using namespace std::chrono_literals;

    gazellemq::client::getSubscriberClient()
        .subscribe("test1", [](std::string&& message) {
            printf("test1 - %s\n", message.c_str());
        })
        .subscribe("test2", [](std::string&& message) {
            printf("test2 - %s\n", message.c_str());
        })
        .connectToHub("ExampleSubscriber", "localhost", 5875);
}
