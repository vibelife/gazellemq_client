#include <latch>
#include <thread>
#include "../client_lib/include/PublisherClient.hpp"

int main() {
    using namespace std::chrono_literals;

    auto& client = gazellemq::client::getPublisherClient();
    client.connectToHub("ExamplePublisher", "localhost", 5875);

    std::latch latch(1);

    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();

    // client.publish("test1", R"({"email":"andre_newman2@hotmail.com","password":"28077485"})");
    // client.publish(888, R"({"email":"paul@akanewmedia.com","password":"metallica"})");

    auto t = std::chrono::high_resolution_clock::now().time_since_epoch();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t);
    printf("Current time: %zu\n", ms.count());

    for (int i{}; i < 500000; ++i) {
        client.publish("test1", R"({"email":"andre_newman2@hotmail.com","password":"28077485"})");
    }

    double elapsed = std::chrono::duration<double>{std::chrono::duration_cast<std::chrono::duration<double>>(std::chrono::high_resolution_clock::now() - t1)}.count();
    printf("Elapsed time: %fs\n", elapsed);

    latch.wait();
    return 0;
}
