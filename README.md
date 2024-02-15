High performance [GazelleMQ](https://github.com/vibelife/gazellemq_server) C++ client libraries.
---

This repo contains an example subscriber, and an example publisher that both connect to GazelleMQ.

The publisher is as simple as 
```
#include <latch>
#include "../client_lib/include/PublisherClient.hpp"

int main() {
  std::latch latch(1);

  auto& client = gazellemq::client::getPublisherClient();
  client.connectToHub("ExamplePublisher", "localhost", 5875);
  client.publish("login", R"([{"email":"d@hotmail.com","pa":"password"},{"email":"p@hotmail.com","password":"metallica"}])");

  latch.wait();
  return 0;
}
```

The subscriber is as simple as
```
#include "../client_lib/include/SubscriberClient.hpp"

int main() {
    gazellemq::client::getSubscriberClient()
        .subscribe("login", [&count](std::string&& message) {
            std::cout << message << std::endl;
        })
        .connectToHub("ExampleSubscriber", "localhost", 5875);
}
```
## Performance tests
Common parameters for all tests
- Intel Core i9-12900KF 64GB RAM
- Ubuntu 22.04
- Message size: 86 bytes
- GazelleMQ server batch buffer size: 32KB (messages less than 32KB are automatically batched together)
- Publisher batch size: 10 messages (up to 10 waiting messages are batched together and published)

### Performance tests #1
- No compiler optimization (GazelleMQ server also not using compiler optimization)
- 1 Publisher is publishing 1,000,000 messages (86MB) to GazelleMQ
- 1 Subscriber is receiving all 1,000,000 messages from GazelleMQ

Results:  All messages received on the subscriber after 430ms - 489ms

### Performance tests #2
- **-O3 compiler optimization** (GazelleMQ server also using -O3)
- 1 Publisher is publishing 1,000,000 messages (86MB) to GazelleMQ
- 1 Subscriber is receiving all 1,000,000 messages from GazelleMQ

Results:  All messages received on the subscriber after 212ms - 233ms

### Performance tests #3
- **-O3 compiler optimization** (GazelleMQ server also using -O3)
- 1 Publisher is publishing 2,000,000 messages (172MB) to GazelleMQ
- 1 Subscriber is receiving all 2,000,000 messages from GazelleMQ

Results:  All messages received on the subscriber after 435ms - 452ms


### Performance tests #4
- **-O3 compiler optimization** (GazelleMQ server also using -O3)
- 1 Publisher is publishing 3,000,000 messages (258MB) to GazelleMQ
- 1 Subscriber is receiving all 3,000,000 messages from GazelleMQ

Results:  All messages received on the subscriber after 645ms - 674ms


### Performance tests #5
- **-O3 compiler optimization** (GazelleMQ server also using -O3)
- 1 Publisher is publishing 4,000,000 messages (344MB) to GazelleMQ
- 1 Subscriber is receiving all 4,000,000 messages from GazelleMQ

Results:  All messages received on the subscriber after 820ms - 881ms


## How to replicate these performance tests
1) Clone https://github.com/vibelife/gazellemq_server compile and run the GazelleMQ server.
2) Clone this repo and change NB_MESSAGES from 1 to 1,000,000 (or whatever number you want) in both the subscriber and publisher main.cpp files.
3) With GazelleMQ server running, run the subscriber first, then run the publisher. Compare the timestamps in the publisher and subscriber console outs.