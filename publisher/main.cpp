#include <latch>
#include <thread>
#include "../client_lib/include/PublisherClient.hpp"

int main() {
    using namespace std::chrono_literals;

    auto& client = gazellemq::client::getPublisherClient();
    client.connectToHub("ExamplePublisher", "localhost", 5875);

    std::latch latch(1);

    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();

    //client.publish(888, R"({"email":"andre_newman2@hotmail.com","password":"28077485"})");
    // client.publish(888, R"({"email":"paul@akanewmedia.com","password":"metallica"})");

    // for (int i{}; i < 1; ++i) {
    //     client.publish(888, R"({"event_handler_id":35,"nbItems":"2","p":1,"poll_id":"KdGjlOoB","q":[{"a":2,"b":5,"c":["%Firefox%"]}],"sessionId":"2vXmHg0vYpRMkTVXLIsP8prx4bMSBAy0","sfd":"1673985427581","workspaceId":"a26c4102-371a-4ba7-bafc-f555aef6ce95","wsid":"???"})");
    // }

    double elapsed = std::chrono::duration<double>{std::chrono::duration_cast<std::chrono::duration<double>>(std::chrono::high_resolution_clock::now() - t1)}.count();
    printf("Elapsed time: %fms\n", elapsed);

    latch.wait();
    return 0;
}
