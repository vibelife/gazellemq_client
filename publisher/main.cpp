#include <latch>
#include <thread>
// #include <jemalloc/jemalloc.h>
#include "../client_lib/include/Client.hpp"
#include "../common/Consts.hpp"

int main() {
    using namespace std::chrono_literals;

    auto& client = gazellemq::client::getClient();
    client.connectToHub();

    std::latch latch(1);

    client.setOnReady([&latch] {
        auto t = std::chrono::high_resolution_clock::now().time_since_epoch();
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t);
        printf("Publishing %d messages: time=%zu\n", bench::consts::NB_MESSAGES, ms.count());
        latch.count_down();
    });

    latch.wait();

    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
    for (int i{}; i < bench::consts::NB_MESSAGES; ++i) {
        client.publish("clientId:Mu2rXoG0I6AUwGBL", R"({"type":"test_type", "value": "test_value"})");
    }

    // malloc_stats_print(NULL, NULL, NULL);

    std::latch{1}.wait();

    return 0;
}
