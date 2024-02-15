High performance C++ client libraries that connect to the GazelleMQ server.
---
1) Contains a publisher that pushes messages to GazelleMQ server
2) Contains a subscriber that receives messages from GazelleMQ server
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
## Performance test #1

- Intel Core i9-12900KF 64GB RAM
- Ubuntu 22.04
- No compiler optimization (GazelleMQ server also NOT using compiler optimization)
- Message size: 86 bytes
- GazelleMQ server batch buffer size: 32KB (messages less than 32KB are automatically batched together)
- Publisher batch size: 10 messages (up to 10 waiting messages are batched together and published)
- 1 Publisher is publishing 1,000,000 messages to GazelleMQ
- 1 Subscriber is receiving all 1,000,000 messages from GazelleMQ

Results:  All messages received on the subscriber after 430ms - 489ms

## Performance test #2

- Intel Core i9-12900KF 64GB RAM
- Ubuntu 22.04
- **-O3 compiler optimization** (GazelleMQ server also using -O3 compiler optimization!)
- Message size: 86 bytes
- GazelleMQ server batch buffer size: 32KB (messages less than 32KB are automatically batched together)
- Publisher batch size: 10 messages (up to 10 waiting messages are batched together and published)
- 1 Publisher is publishing 1,000,000 messages to GazelleMQ
- 1 Subscriber is receiving all 1,000,000 messages from GazelleMQ

Results:  All messages received on the subscriber after 210ms - 230ms

## How to replicate these performance tests
1) Clone https://github.com/vibelife/gazellemq_server compile and run the GazelleMQ server.
2) Clone this repo and change NB_MESSAGES from 1 to 1000000 (or whatever number you want) in both the subscriber and publisher main.cpp files.
