#include <latch>
#include <atomic>
#include "../client_lib/include/Client.hpp"
#include "Random.hpp"
#include "../common/Consts.hpp"

int main() {
    using namespace std::chrono_literals;

    std::atomic<long> count{0};

    gazellemq::client::getClient()
        .connectToHub()
        .subscribe("clientId:Mu2rXoG0I6AUwGBL", [&count](std::string&& message) {
            if (++count == bench::consts::NB_MESSAGES) {
                auto t = std::chrono::high_resolution_clock::now().time_since_epoch();
                auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t);
                printf("Received all %d messages: time=%zu\n", bench::consts::NB_MESSAGES, ms.count());
                count = 0;
            }
        });

    std::latch{1}.wait();
}
