High performance C++ client libraries that connect to the GazelleMQ server.
---
1) Contains a publisher that pushes messages to GazelleMQ server
2) Contains a subscriber that receices messages from GazelleMQ server
3) Contains an example publisher, and an example subscriber

The publisher is as simple as 
```
#include <latch>
#include "../client_lib/include/PublisherClient.hpp"

int main() {
  std::latch latch(1);

  auto& client = gazellemq::client::getPublisherClient();
  client.connectToHub("ExamplePublisher", "localhost", 5875);
  client.publish("test1", R"([{"email":"d@hotmail.com","pa":"password"},{"email":"p@hotmail.com","password":"metallica"}])");

  latch.wait();
  return 0;
}
```

The subscriber is as simple as
```
#include "../client_lib/include/SubscriberClient.hpp"

int main() {
    gazellemq::client::getSubscriberClient()
        .subscribe("test1", [&count](std::string&& message) {
            std::cout << message << std::endl;
        })
        .connectToHub("ExampleSubscriber", "localhost", 5875);
}
```


