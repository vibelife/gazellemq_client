#ifndef GAZELLEMQ_CLIENT_CLIENT_HPP
#define GAZELLEMQ_CLIENT_CLIENT_HPP

#include <iostream>
#include <netdb.h>
#include <liburing.h>
#include <thread>
#include <condition_variable>
#include <vector>
#include <netinet/tcp.h>
#include <cstring>
#include <sys/epoll.h>
#include <functional>
#include <bits/ranges_algo.h>

#include <algorithm>
#include <array>
#include <random>
#include <string>
#include <execution>

#include "MPMCQueue/MPMCQueue.hpp"

namespace gazellemq::client {
    class EZClient {
    protected:
        enum ClientStep {
            ClientStep_NotSet,
            ClientStep_Ready,
            ClientStep_ConnectToHub,
            ClientStep_EPollSetup,
            ClientStep_SendingMessage,
            ClientStep_SendingIntent,
            ClientStep_SendingName,
            ClientStep_SendingSubscriptions,
            ClientStep_Ack,
            ClientStep_CommandAck,
            ClientStep_ReceiveData,
            ClientStep_Disconnect,
            ClientStep_Reconnect,
        };

        static constexpr addrinfo hints{0, AF_INET, SOCK_STREAM, 0, 0, nullptr, nullptr, nullptr};
        static constexpr auto CLIENT_INTENT = "C\r";
        static constexpr auto MAX_READ_BUF = 8192;
        static constexpr auto TIMEOUT = -62;

        ClientStep step{ClientStep_NotSet};

        io_uring ring{};
        int epfd{};
        int fd{};
        std::string hubHost;
        int hubPort{};
        bool isConnected{false};
        bool isSetupComplete{false};
        bool wasConnectToHubCalled{false};
        std::atomic_flag isRunning{true};

        std::string clientName;
        struct gaicb* gai{};

        size_t const eventQueueDepth;
        size_t const maxEventBatch;
        std::string writeBuffer;
        char readBuffer[MAX_READ_BUF]{};

    public:
        explicit EZClient(
                int const eventQueueDepth = 8192,
                int const maxEventBatch = 32
        ) noexcept:
                eventQueueDepth(eventQueueDepth),
                maxEventBatch(maxEventBatch)
        {}

        virtual ~EZClient() {
            isRunning.clear();
        }
    public:
        /**
         * Signal handler
         * @param signo
         */
        static void sigintHandler(int signo) {
            printf("^C pressed. Shutting down\n");
            exit(0);
        }

        /**
         * Asynchronously connects to the gazelle server at the host and port specified.
         * @param host - Host of your gazelle server
         * @param port - Port of your gazelle server
         */
        void connectToHub(char const* name, char const* host, int const port) {
            clientName.append(name);
            hubPort = port;

            if (!wasConnectToHubCalled) {
                wasConnectToHubCalled = true;
                init();
                connectToServer(host, port);
            }
        }

    protected:
        /**
         * Initialization code here
         */
        virtual void init() = 0;

        /**
         * Error handler
         * @param msg
         */
        static void printError(char const* msg) {
            printf("Error: %s\n", msg);
        }

        /**
         * Error handler
         * @param msg
         */
        static void printError(char const* msg, int err) {
            printf("%s\n%s\n", msg, strerror(-err));
        }

        void clearGaiVector(std::vector<gaicb*>&& v) {
            for (gaicb* g: v) {
                delete g;
            }
            v.clear();
            gai = nullptr;
        }

        /**
         * Connects to the hub
         * @return
         */
        bool connect() {
            const std::string strPort{std::to_string(hubPort)};
            gai = new gaicb{hubHost.c_str(), strPort.c_str(), &hints, nullptr};
            std::vector<gaicb*> hosts;
            hosts.emplace_back(gai);
            getaddrinfo_a(GAI_WAIT, hosts.data(), static_cast<int>(hosts.size()), nullptr);

            const auto hi = this->gai;
            int ret = gai_error(hi);
            if (ret == 0) {
                if (hi->ar_result == nullptr) {
                    printf("Could not connect to %s:%d\n", hubHost.c_str(), hubPort);
                    clearGaiVector(std::move(hosts));
                    return false;
                }

                // try to create the socket
                for (const addrinfo* rp = hi->ar_result; rp != nullptr; rp = rp->ai_next) {
                    fd = socket(rp->ai_family, rp->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC, rp->ai_protocol);

                    if (fd == -1) {
                        continue;
                    }

                    // https://news.ycombinator.com/item?id=10607422
                    // https://rigtorp.se/sockets/
                    int opt = 1;
                    constexpr size_t optLen = sizeof(opt);

                    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, optLen) == -1) {
                        printError("setsockopt() [TCP_NODELAY]");
                        clearGaiVector(std::move(hosts));
                        return false;
                    }

                    // enable TCP keep alive
                    opt = 1;
                    if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &opt, optLen) == -1) {
                        printError("setsockopt() [SO_KEEPALIVE]");
                        clearGaiVector(std::move(hosts));
                        return false;
                    }

                    // set the number of idle seconds before sending keepalive probes
                    opt = 5;
                    if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &opt, optLen) == -1) {
                        printError("setsockopt() [TCP_KEEPIDLE]");
                        clearGaiVector(std::move(hosts));
                        return false;
                    }

                    // set the number of seconds between keepalive probes
                    opt = 1;
                    if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &opt, optLen) == -1) {
                        printError("setsockopt() [TCP_KEEPINTVL]");
                        clearGaiVector(std::move(hosts));
                        return false;
                    }

                    // set the max number of keepalive probes before dropping the connection
                    opt = 2;
                    if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &opt, optLen) == -1) {
                        printError("setsockopt() [TCP_KEEPCNT]");
                        clearGaiVector(std::move(hosts));
                        return false;
                    }


                    const auto sqe = io_uring_get_sqe(&ring);
                    io_uring_prep_connect(sqe, fd, this->gai->ar_result->ai_addr, this->gai->ar_result->ai_addrlen);
                    // io_uring_sqe_set_data(sqe, this);

                    step = ClientStep_ConnectToHub;
                    io_uring_sqe_set_data(sqe, this);
                    io_uring_submit(&ring);
                    freeaddrinfo(this->gai->ar_result);
                    break;
                }
            } else {
                printError(gai_strerror(ret));
                clearGaiVector(std::move(hosts));
                return false;
            }
            clearGaiVector(std::move(hosts));
            return true;
        }

        /**
         * Asynchronously connects to the gazelle server at the host and port specified.
         * @param host - Host of your gazelle server
         * @param port - Port of your gazelle server
         */
        void connectToServer(char const* host, int const port) {
            hubHost = std::string{host};
            hubPort = port;

            runInBackground();
        }

        virtual void runInBackground() = 0;

        /**
         * Sets up epoll
         * @param res
         */
        bool beginEPollSetup(const int res) {
            if (res < 0) {
                printError("Could not connect to server, retrying...");
                // Cant continue in this state
                return false;
            } else {
                isConnected = true;

                epfd = epoll_create1(0);
                if (epfd < 0) {
                    printError(strerror(-epfd));
                    exit(0);
                }

                io_uring_sqe* sqe = io_uring_get_sqe(&ring);
                struct epoll_event ev{};
                ev.events = EPOLLIN | EPOLLOUT | EPOLLHUP | EPOLLERR | EPOLLRDHUP;
                ev.data.fd = fd;
                io_uring_prep_epoll_ctl(sqe, epfd, fd, EPOLL_CTL_ADD, &ev);

                step = ClientStep_EPollSetup;
                io_uring_sqe_set_data(sqe, this);
                io_uring_submit(&ring);

                return true;
            }
        }

        /**
         * Epoll has been set up. Next we send the intent.
         * @param res
         */
        bool onEPollSetupComplete(const int res) {
            if (res == 0) {
                writeBuffer.clear();
                writeBuffer.append(getIntent());

                beginSendIntent();
                return true;
            } else {
                printError("epoll setup failed\n");
                // Cant continue in this state
                exit(0);
                return false;
            }
        }

        virtual char const* getIntent() const = 0;

        /**
         * Submit a send data request
         */
        void beginSendIntent() {
            io_uring_sqe* sqe = io_uring_get_sqe(&ring);
            io_uring_prep_send(sqe, fd, writeBuffer.c_str(), writeBuffer.size(), 0);

            step = ClientStep_SendingIntent;
            io_uring_sqe_set_data(sqe, this);
            io_uring_submit(&ring);
        }

        /**
         * At this point we are done communicating with the hub. Now messages can be published.
         * @param res
         */
        bool onSendIntentComplete(const int res) {
            if (res == 0) {
                return false;
            }

            writeBuffer.erase(0, res);
            if (writeBuffer.empty()) {
                writeBuffer.clear();
                writeBuffer.append(clientName);
                writeBuffer.append("\r");
                beginSendName();
            } else {
                beginSendIntent();
            }
            return true;
        }

        /**
         * Sends the name to the hub
         */
        void beginSendName() {
            io_uring_sqe* sqe = io_uring_get_sqe(&ring);
            io_uring_prep_send(sqe, fd, writeBuffer.c_str(), writeBuffer.size(), 0);

            step = ClientStep_SendingName;
            io_uring_sqe_set_data(sqe, this);
            io_uring_submit(&ring);
        }

        /**
         * Checks if we are done sending the name bytes
         * @param res
         * @return
         */
        bool onSendNameComplete(const int res) {
            writeBuffer.erase(0, res);
            if (!writeBuffer.empty()) {
                beginSendName();
                return true;
            }
            afterSendNameComplete();
            return true;
        }

        /**
         * Disconnects from the server
         */
        void beginDisconnect() {
            io_uring_sqe* sqe = io_uring_get_sqe(&ring);
            io_uring_prep_close(sqe, fd);

            step = ClientStep_Disconnect;
            io_uring_sqe_set_data(sqe, this);
            io_uring_submit(&ring);
        }

        void onDisconnectComplete() {
            isConnected = false;
        }

        /**
         * Tries to connect to the hub
         */
        void beginReconnectToServer() {
            if (step != ClientStep_Reconnect) {
                step = ClientStep_Reconnect;
                isConnected = false;

                printf("Trying to connect to hub...\n");
                close(fd);
                close(epfd);
                connect();
            }
        }

        virtual void afterSendNameComplete() = 0;

        virtual void doEventLoop() = 0;
    };

    class SubClient final : public EZClient {
    private:
        class MessageHandler {
        public:
            std::function<void(std::string&&)> const callback;
            std::string messageTypeId;

            static MessageHandler create(std::string&& messageTypeId, std::function<void(std::string&&)>&& callback) {
                MessageHandler m{std::move(callback), std::move(messageTypeId)};

                return std::move(m);
            }
        };

        struct Message {
            std::string msgType;
            std::string message;
        };

        enum ParseState {
            ParseState_notSet,
            ParseState_messageType,
            ParseState_messageContentLength,
            ParseState_messageContent,
        };

        ParseState parseState{ParseState_notSet};
        rigtorp::MPMCQueue<Message> messages;
        std::vector<MessageHandler> handlers;
        std::vector<std::jthread> handlerThreads;
        int const nbMessageHandlerThreads;

        std::condition_variable cvMessageQueue;
        std::mutex mMessageQueue;
        std::atomic_flag hasMessages{false};
        std::atomic_flag isReady{false};
        std::function<void()> onReadyFn{nullptr};
        char ackBuffer[1]{};

        std::string message;
        std::string messageType;
        std::string messageLengthBuffer;
        std::string messageContent;
        size_t messageContentLength{};
        size_t nbContentBytesRead{};
        std::jthread bgThread;
    public:
        explicit SubClient(
                int const messagesQueueSize = 500000,
                int const nbMessageHandlerThreads = 8
            )
            : nbMessageHandlerThreads(nbMessageHandlerThreads),
              messages(std::max(messagesQueueSize, 1)),
              EZClient()
            {}

        /**
         * Initialization code here
         */
        void init() override {
            signal(SIGINT, sigintHandler);
            io_uring_queue_init(eventQueueDepth, &ring, 0);
            monitorSubscriptionMessageQueue();
        }
    public:
        /**
         * Subscribes to a single message type.
         * @param messageTypeId - The message type ID
         * @param callback - Callback when an event is published that has the passed in [messageTypeId]
         */
        void subscribe(std::string const& messageTypeId, std::function<void(std::string&&)>&& callback) {
            handlers.emplace_back(MessageHandler::create(std::string(messageTypeId), std::move(callback)));
        }

        SubClient& setOnReady(std::function<void()>&& fn) {
            this->onReadyFn = std::move(fn);
            return *this;
        }
    protected:
        void afterSendNameComplete() override {
            beginReceiveAck();
        }

        void runInBackground() override {
            bgThread = std::jthread([this]() {
                if (connect()) {
                    doEventLoop();
                }
            });
        }

        [[nodiscard]] const char *getIntent() const override {
            return "S\r";
        }

    private:
        /**
         * Receives acknowledgment from the server
         */
        void beginReceiveAck() {
            io_uring_sqe* sqe = io_uring_get_sqe(&ring);
            io_uring_prep_recv(sqe, fd, ackBuffer, 1, 0);

            step = ClientStep_Ack;
            io_uring_sqe_set_data(sqe, this);
            io_uring_submit(&ring);
        }

        void onReceiveAckComplete(int res) {
            beginReceiveData();
        }

        /**
         * Receives data from the hub
         */
        void beginReceiveData() {
            io_uring_sqe *sqe = io_uring_get_sqe(&ring);
            io_uring_prep_recv(sqe, fd, readBuffer, MAX_READ_BUF, 0);

            step = ClientStep_ReceiveData;
            io_uring_sqe_set_data(sqe, this);
            io_uring_submit(&ring);

            if (!isReady.test()) {
                isReady.test_and_set();
                if (onReadyFn != nullptr) {
                    onReadyFn();
                }
            }
        }

        /**
         * Checks if we are done receiving data
         * @param res
         */
        void onReceiveDataComplete(int res) {
            if (res <= 0) {
                // disconnected
                beginDisconnect();
            } else {
                parseMessage(readBuffer, res);
                // receive more data
                beginReceiveData();
            }
        }

        void parseMessage(char const* buffer, size_t bufferLength) {
            for (size_t i{0}; i < bufferLength; ++i) {
                char ch{buffer[i]};
                if (parseState == ParseState_notSet || parseState == ParseState_messageType) {
                    if (ch == '|') {
                        parseState = ParseState_messageContentLength;
                        continue;
                    } else {
                        parseState = ParseState_messageType;
                        messageType.push_back(ch);
                    }
                } else if (parseState == ParseState_messageContentLength) {
                    if (ch == '|') {
                        messageContentLength = std::stoul(messageLengthBuffer);
                        parseState = ParseState_messageContent;
                        continue;
                    } else {
                        messageLengthBuffer.push_back(ch);
                    }
                } else if (parseState == ParseState_messageContent) {
                    size_t nbCharsNeeded {messageContentLength - nbContentBytesRead};
                    // add as many characters as possible in bulk

                    if ((i + nbCharsNeeded) <= bufferLength) {
                        messageContent.append(&buffer[i], nbCharsNeeded);
                    } else {
                        nbCharsNeeded = bufferLength - i;
                        messageContent.append(&buffer[i], nbCharsNeeded);
                    }

                    i += nbCharsNeeded - 1;
                    nbContentBytesRead += nbCharsNeeded;

                    if (messageContentLength == nbContentBytesRead) {
                        // Done parsing
                        messages.push(Message{std::move(messageType), std::move(messageContent)});
                        notify();

                        messageContentLength = 0;
                        nbContentBytesRead = 0;
                        messageContent.clear();
                        messageLengthBuffer.clear();
                        messageType.clear();
                        parseState = ParseState_notSet;
                    }
                }
            }
        }

        /**
         * Alerts the thread that there is pending data
         */
        void notify() {
            if (!hasMessages.test()) {
                std::lock_guard lock{mMessageQueue};
                hasMessages.test_and_set();
                cvMessageQueue.notify_one();
            }
        }

        /**
         * Monitors the message queue and handles them when they come in
         */
        void monitorSubscriptionMessageQueue() {
            for (int i{}; i < nbMessageHandlerThreads; ++i) {
                handlerThreads.emplace_back([this]() {
                    while (isRunning.test()) {
                        std::unique_lock uLock{mMessageQueue};
                        cvMessageQueue.wait(uLock, [this]() { return hasMessages.test(); });
                        uLock.unlock();

                        Message m;
                        if (messages.try_pop(m)) {
                            for (size_t i{}; i < handlers.size(); ++i) {
                                auto& handler = handlers.at(i);
                                if (handler.messageTypeId == m.msgType) {
                                    handler.callback(std::move(m.message));
                                }
                            }
                        }

                        {
                            std::lock_guard lockGuard{mMessageQueue};
                            if (messages.empty()) {
                                hasMessages.clear();
                            }
                        }
                    }
                });
            }
        }

    protected:
        /**
         * Runs the event loop
         */
        void doEventLoop() override {
            using namespace std::chrono_literals;

            std::vector<io_uring_cqe*> cqes{};
            cqes.reserve(maxEventBatch);
            cqes.insert(cqes.begin(), maxEventBatch, nullptr);

            __kernel_timespec ts{.tv_sec = 2, .tv_nsec = 0};

            while (isRunning.test()) {
                int ret = io_uring_wait_cqe_timeout(&ring, cqes.data(), &ts);
                if (ret == -SIGILL) {
                    continue;
                }

                if (ret < 0) {
                    if (ret == TIMEOUT) {
                        if (!isConnected) {
                            beginReconnectToServer();
                        }
                    } else {
                        printError("io_uring_wait_cqe_nr(...)", ret);
                        return;
                    }
                }

                if (ret == TIMEOUT) {
                    continue;
                }

                for (auto &cqe: cqes) {
                    if (cqe != nullptr) {
                        int res = cqe->res;
                        if (res == -EAGAIN) {
                            io_uring_cqe_seen(&ring, cqe);
                            continue;
                        }

                        auto* pObject = static_cast<SubClient*>(io_uring_cqe_get_data(cqe));

                        switch (pObject->step) {
                            case ClientStep_ConnectToHub:
                                pObject->beginEPollSetup(res);
                                break;
                            case ClientStep_EPollSetup:
                                pObject->onEPollSetupComplete(res);
                                break;
                            case ClientStep_SendingIntent:
                                pObject->onSendIntentComplete(res);
                                break;
                            case ClientStep_SendingName:
                                pObject->onSendNameComplete(res);
                                break;
                            case ClientStep_Ack:
                                pObject->onReceiveAckComplete(res);
                                break;
                            case ClientStep_ReceiveData:
                                pObject->onReceiveDataComplete(res);
                                break;
                            case ClientStep_Disconnect:
                                pObject->onDisconnectComplete();
                                break;
                            default:
                                break;
                        }
                    }

                    io_uring_cqe_seen(&ring, cqe);
                }
            }

            io_uring_queue_exit(&ring);
            close(fd);
            close(epfd);
        }

    };

    class PubClient final : public EZClient {
    private:
        std::function<void()> onReadyFn{nullptr};
        unsigned int messageBatchSize;
        rigtorp::MPMCQueue<std::string> queue;
        std::vector<int> raisedMessageTypeIds;
        char ackBuffer[1]{};
        std::mutex mQueue;
        std::condition_variable cvQueue;
        std::atomic_flag hasPendingData{false};
        std::atomic_flag isReady{false};
        std::string nextBatch;
        std::jthread bgThread;
    public:
        explicit PubClient(
                int const queueDepth = 500000,
                int const eventQueueDepth = 8192,
                int const maxEventBatch = 32,
                int const msgBatchSize = 1
        ) noexcept:
                EZClient(eventQueueDepth, maxEventBatch),
                messageBatchSize(std::max(msgBatchSize, 1)),
                queue(queueDepth)
        {
            writeBuffer.reserve(256);
        }
        ~PubClient() override {
            isRunning.clear();
        }
    public:
        /**
         * Publishes a message to the hub. Subscribers of [messageType] will receive the message.
         * @param messageType - The message type. Subscribers must subscribe to this exact string to handle the message.
         * @param messageContent - The message contents.
         */
        void publish(std::string&& messageType, std::string&& messageContent) {
            std::string msg;
            msg.reserve(messageType.size() + messageContent.size() + 16);
            msg.append(messageType);
            msg.push_back('|');
            msg.append(std::to_string(messageContent.size()));
            msg.push_back('|');
            msg.append(messageContent);

            queue.push(std::move(msg));

            notify();
        }

        PubClient& setOnReady(std::function<void()>&& fn) {
            this->onReadyFn = std::move(fn);
            return *this;
        }

    protected:
        void afterSendNameComplete() override {
            beginReceiveAck();
        }

        void runInBackground() override {
            bgThread = std::jthread([this]() {
                if (connect()) {
                    doEventLoop();
                }
            });
        }

        [[nodiscard]] const char *getIntent() const override {
            return "P\r";
        }
    private:
        /**
         * Alerts the thread that there is pending data
         */
        void notify() {
            if (!hasPendingData.test()) {
                std::lock_guard lock{mQueue};
                hasPendingData.test_and_set();
                cvQueue.notify_all();
            }
        }

        /**
         * Receives acknowledgment from the server
         */
        void beginReceiveAck() {
            io_uring_sqe* sqe = io_uring_get_sqe(&ring);
            io_uring_prep_recv(sqe, fd, ackBuffer, 1, 0);

            step = ClientStep_Ack;
            io_uring_sqe_set_data(sqe, this);
            io_uring_submit(&ring);
        }

        void onReceiveAckComplete(int res) {
            step = ClientStep_Ready;
            isReady.test_and_set();
            if (onReadyFn != nullptr) {
                onReadyFn();
            }
        }

        /**
         * Sends data to the hub.
         */
        void beginSendData() {
            io_uring_sqe* sqe = io_uring_get_sqe(&ring);
            io_uring_prep_send(sqe, fd, writeBuffer.c_str(), writeBuffer.size(), 0);

            step = ClientStep_SendingMessage;
            io_uring_sqe_set_data(sqe, this);
            io_uring_submit(&ring);
        }

        /**
         * Checks if more bytes need to be sent. Returns true if there is more sending to be done, false otherwise.
         * @param res
         */
        bool onDataSent(const int res) {
            if (res < 1) {
                if (res < 0) {
                    printError("Could not write data", res);
                }
                return false;
            } else {
                writeBuffer.erase(0, res);
                if (writeBuffer.empty()) {
                    {
                        std::lock_guard lockGuard{mQueue};
                        if (queue.empty()) {
                            hasPendingData.clear();
                        }
                    }

                    return false;
                } else {
                    // still need to send some bytes
                    beginSendData();
                }
            }
            return true;
        }

        /**
         * Drains the queue of messages and sends them. Returns true if it was sent, false if the queue is empty.
         * @return
         */
        bool drainQueue() {
            std::string tmp;
            nextBatch.clear();

            int i{0};
            while ((i != messageBatchSize) && queue.try_pop(tmp)) {
                nextBatch.append(tmp);
                ++i;
            }

            std::swap(writeBuffer, nextBatch);

            if (!writeBuffer.empty()) {
                beginSendData();
                return true;
            }

            return false;
        }

        /**
         * Initialization code here
         */
        void init() override {
            signal(SIGINT, sigintHandler);
            io_uring_queue_init(eventQueueDepth, &ring, 0);
        }

        /**
         * Runs the event loop
         */
        void doEventLoop() override {
            using namespace std::chrono_literals;

            std::vector<io_uring_cqe*> cqes{};
            cqes.reserve(maxEventBatch);
            cqes.insert(cqes.begin(), maxEventBatch, nullptr);

            __kernel_timespec ts{.tv_sec = 2, .tv_nsec = 0};

            outer:
            while (isRunning.test()) {
                if (isReady.test()) {
                    std::unique_lock uniqueLock{mQueue};
                    bool didTimeout{!cvQueue.wait_for(uniqueLock, 2s, [this]() { return hasPendingData.test() || !isRunning.test(); })};
                    if (!isRunning.test()) break;

                    uniqueLock.unlock();

                    if (didTimeout) {
                        // Check if we need to reconnect to the hub
                        if (fd == 0) {
                            connect();
                        } else {
                            goto outer;
                        }
                    } else if (!drainQueue()) {
                        goto outer;
                    }
                }

                while (isRunning.test()) {
                    int ret = io_uring_wait_cqe_timeout(&ring, cqes.data(), &ts);
                    if (ret == -SIGILL) {
                        continue;
                    }

                    if (ret < 0) {
                        if (ret == TIMEOUT) {
                            if (!isConnected) {
                                beginReconnectToServer();
                            }
                        } else {
                            printError("io_uring_wait_cqe_nr(...)", ret);
                            return;
                        }
                    }

                    if (ret == TIMEOUT) {
                        continue;
                    }

                    for (auto& cqe: cqes) {
                        if (cqe != nullptr) {
                            int res = cqe->res;
                            if (res == -EAGAIN) {
                                io_uring_cqe_seen(&ring, cqe);
                                continue;
                            }

                            auto* pObject = static_cast<PubClient*>(io_uring_cqe_get_data(cqe));

                            switch (pObject->step)   {
                                case ClientStep_ConnectToHub:
                                    if (pObject->beginEPollSetup(res)) {
                                        break;
                                    } else {
                                        io_uring_cqe_seen(&ring, cqe);
                                        goto outer;
                                    }
                                case ClientStep_EPollSetup:
                                    if (pObject->onEPollSetupComplete(res)) {
                                        break;
                                    } else {
                                        io_uring_cqe_seen(&ring, cqe);
                                        goto outer;
                                    }
                                case ClientStep_SendingIntent:
                                    if (pObject->onSendIntentComplete(res)) {
                                        break;
                                    } else {
                                        io_uring_cqe_seen(&ring, cqe);
                                        goto outer;
                                    }
                                case ClientStep_SendingName:
                                    if (pObject->onSendNameComplete(res)) {
                                        break;
                                    } else {
                                        io_uring_cqe_seen(&ring, cqe);
                                        goto outer;
                                    }
                                case ClientStep_Ack:
                                    pObject->onReceiveAckComplete(res);
                                    io_uring_cqe_seen(&ring, cqe);
                                    goto outer;
                                case ClientStep_SendingMessage:
                                    if (pObject->onDataSent(res)) {
                                        break;
                                    } else {
                                        io_uring_cqe_seen(&ring, cqe);
                                        goto outer;
                                    }
                                default:
                                    break;
                            }
                        }

                        io_uring_cqe_seen(&ring, cqe);
                    }
                }
            }

            io_uring_queue_exit(&ring);
            close(fd);
            close(epfd);
        }
    };

    class CommandClient final : public EZClient {
    private:
        std::function<void()> onReadyFn{nullptr};
        unsigned int messageBatchSize;
        rigtorp::MPMCQueue<std::string> queue;
        char ackBuffer[1]{};
        std::mutex mQueue;
        std::condition_variable cvQueue;
        std::atomic_flag hasPendingData{false};
        std::atomic_flag isReady{false};
        std::string nextBatch;
        std::jthread bgThread;

        std::string subscriberName;
        std::unordered_map<std::string, std::function<void()>> subscriptions;
        std::unordered_map<std::string, std::function<void()>> pendingSubscriptions;
    public:
        explicit CommandClient(
            int const queueDepth = 8,
            int const eventQueueDepth = 4,
            int const maxEventBatch = 2,
            int const msgBatchSize = 1
        ) noexcept:
            EZClient(eventQueueDepth, maxEventBatch),
            messageBatchSize(std::max(msgBatchSize, 1)),
            queue(queueDepth)
        {
            writeBuffer.reserve(256);
        }
        ~CommandClient() override {
            isRunning.clear();
        }
    public:
        void setSubscriberName(std::string const& name) {
            this->subscriberName = name;
        }

        /**
         * Subscribes to a single message type.
         * @param messageTypeId - The message type ID
         * @param onAddedCallback - callback to run once the subscription is added
         */
        void subscribe(std::string const& messageTypeId, std::function<void()>&& onAddedCallback) {
            if (!isSetupComplete) {
                this->pendingSubscriptions.emplace(messageTypeId, std::move(onAddedCallback));
            } else {
                std::string msg{this->subscriberName};
                msg.append("|subscribe|");
                msg.append(messageTypeId);

                subscriptions.emplace(messageTypeId, std::move(onAddedCallback));

                queue.push(std::move(msg));
                notify();
            }
        }

        CommandClient& setOnReady(std::function<void()>&& fn) {
            this->onReadyFn = std::move(fn);
            return *this;
        }

    protected:
        void init() override {
            signal(SIGINT, sigintHandler);
            io_uring_queue_init(eventQueueDepth, &ring, 0);
        }

        void runInBackground() override {
            bgThread = std::jthread([this]() {
                if (connect()) {
                    doEventLoop();
                }
            });
        }

        const char * getIntent() const override {
            return "C\r";
        }

        void afterSendNameComplete() override {
            beginReceiveAck();
        }

        /**
         * Receives acknowledgment from the server
         */
        void beginReceiveAck() {
            io_uring_sqe* sqe = io_uring_get_sqe(&ring);
            io_uring_prep_recv(sqe, fd, ackBuffer, 1, 0);

            step = ClientStep_Ack;
            io_uring_sqe_set_data(sqe, this);
            io_uring_submit(&ring);
        }

        void onReceiveAckComplete(int res) {
            step = ClientStep_Ready;
            isReady.test_and_set();
            isSetupComplete = true;
            if (onReadyFn != nullptr) {
                onReadyFn();
            }

            for (auto&[fst, snd]: pendingSubscriptions) {
                std::string messageType{fst};
                std::function<void()> cb = std::move(snd);
                subscribe(messageType, std::move(cb));
            }

            pendingSubscriptions.clear();
        }


        /**
         * Sends data to the hub.
         */
        void beginSendData() {
            io_uring_sqe* sqe = io_uring_get_sqe(&ring);
            io_uring_prep_send(sqe, fd, writeBuffer.c_str(), writeBuffer.size(), 0);

            step = ClientStep_SendingMessage;
            io_uring_sqe_set_data(sqe, this);
            io_uring_submit(&ring);
        }

        /**
         * Checks if more bytes need to be sent. Returns true if there is more sending to be done, false otherwise.
         * @param res
         */
        void onDataSent(int const res) {
            if (res < 1) {
                if (res < 0) {
                    printError("Could not write data", res);
                }
            } else {
                writeBuffer.erase(0, res);
                if (writeBuffer.empty()) {
                    {
                        std::lock_guard lockGuard{mQueue};
                        if (queue.empty()) {
                            hasPendingData.clear();
                        }
                    }

                    beginReceiveCommandAck();
                } else {
                    // still need to send some bytes
                    beginSendData();
                }
            }
        }

        /**
         * Receives acknowledgment from the server
         */
        void beginReceiveCommandAck() {
            io_uring_sqe* sqe = io_uring_get_sqe(&ring);
            io_uring_prep_recv(sqe, fd, ackBuffer, 1, 0);

            step = ClientStep_CommandAck;
            io_uring_sqe_set_data(sqe, this);
            io_uring_submit(&ring);
        }

        void onCommandAckReceived(int const res) {
            if (!subscriptions.empty()) {
                // call each handler
                for (auto const& subscription : subscriptions) {
                    if (subscription.second != nullptr) {
                        subscription.second();
                    }
                }
                subscriptions.clear();
            }
        }

        /**
         * Alerts the thread that there is pending data
         */
        void notify() {
            if (!hasPendingData.test()) {
                std::lock_guard lock{mQueue};
                hasPendingData.test_and_set();
                cvQueue.notify_all();
            }
        }

        /**
         * Drains the queue of messages and sends them. Returns true if it was sent, false if the queue is empty.
         * @return
         */
        bool drainQueue() {
            if (!isSetupComplete) return false;

            std::string tmp;
            nextBatch.clear();

            int i{0};
            while ((i != messageBatchSize) && queue.try_pop(tmp)) {
                if (!nextBatch.empty()) {
                    nextBatch.append("\r");
                }
                nextBatch.append(tmp);
                ++i;
            }

            std::swap(writeBuffer, nextBatch);

            if (!writeBuffer.empty()) {
                writeBuffer.append("\r");
                beginSendData();
                return true;
            }

            return false;
        }

        void doEventLoop() override {
            using namespace std::chrono_literals;

            std::vector<io_uring_cqe*> cqes{};
            cqes.reserve(maxEventBatch);
            cqes.insert(cqes.begin(), maxEventBatch, nullptr);

            __kernel_timespec ts{.tv_sec = 2, .tv_nsec = 0};

            outer:
            while (isRunning.test()) {
                if (isReady.test()) {
                    std::unique_lock uniqueLock{mQueue};
                    bool didTimeout{!cvQueue.wait_for(uniqueLock, 2s, [this]() { return hasPendingData.test() || !isRunning.test(); })};
                    if (!isRunning.test()) break;

                    uniqueLock.unlock();

                    if (didTimeout) {
                        // Check if we need to reconnect to the hub
                        if (fd == 0) {
                            connect();
                        } else {
                            goto outer;
                        }
                    } else if (!drainQueue()) {
                        goto outer;
                    }
                }

                while (isRunning.test()) {
                    int ret = io_uring_wait_cqe_timeout(&ring, cqes.data(), &ts);
                    if (ret == -SIGILL) {
                        continue;
                    }

                    if (ret < 0) {
                        if (ret == TIMEOUT) {
                            if (!isConnected) {
                                beginReconnectToServer();
                            }
                        } else {
                            printError("io_uring_wait_cqe_nr(...)", ret);
                            return;
                        }
                    }

                    if (ret == TIMEOUT) {
                        continue;
                    }

                    for (auto& cqe: cqes) {
                        if (cqe != nullptr) {
                            int res = cqe->res;
                            if (res == -EAGAIN) {
                                io_uring_cqe_seen(&ring, cqe);
                                continue;
                            }

                            auto* pObject = static_cast<CommandClient*>(io_uring_cqe_get_data(cqe));

                            switch (pObject->step)   {
                                case ClientStep_ConnectToHub:
                                    if (pObject->beginEPollSetup(res)) {
                                        break;
                                    } else {
                                        io_uring_cqe_seen(&ring, cqe);
                                        goto outer;
                                    }
                                case ClientStep_EPollSetup:
                                    if (pObject->onEPollSetupComplete(res)) {
                                        break;
                                    } else {
                                        io_uring_cqe_seen(&ring, cqe);
                                        goto outer;
                                    }
                                case ClientStep_SendingIntent:
                                    if (pObject->onSendIntentComplete(res)) {
                                        break;
                                    } else {
                                        io_uring_cqe_seen(&ring, cqe);
                                        goto outer;
                                    }
                                case ClientStep_SendingName:
                                    if (pObject->onSendNameComplete(res)) {
                                        break;
                                    } else {
                                        io_uring_cqe_seen(&ring, cqe);
                                        goto outer;
                                    }
                                case ClientStep_Ack:
                                    pObject->onReceiveAckComplete(res);
                                    io_uring_cqe_seen(&ring, cqe);
                                    goto outer;
                                case ClientStep_SendingMessage:
                                    pObject->onDataSent(res);
                                    break;
                                case ClientStep_CommandAck:
                                    pObject->onCommandAckReceived(res);
                                    io_uring_cqe_seen(&ring, cqe);
                                    goto outer;
                                default:
                                    break;
                            }
                        }

                        io_uring_cqe_seen(&ring, cqe);
                    }
                }
            }

            io_uring_queue_exit(&ring);
            close(fd);
            close(epfd);
        }
    };

    class Client final {
    private:
        SubClient subClient;
        PubClient pubClient;
        CommandClient commandClient;
        std::atomic<int> isReady{0};
        std::function<void()> onReadyFn{nullptr};
        int id{};
        bool connectCalled{};
    public:
        explicit Client(int const queueDepth = 32, int const nbThreads = 2)
            : subClient(SubClient{queueDepth, nbThreads}),
              pubClient(PubClient{queueDepth, queueDepth}),
              commandClient(CommandClient{})
        {}

        Client& setOnReady(std::function<void()>&& fn) {
            this->onReadyFn = std::move(fn);
            return *this;
        }

        template <typename T = std::mt19937> static auto random_generator() -> T {
            auto constexpr seed_bytes = sizeof(typename T::result_type) * T::state_size;
            auto constexpr seed_len = seed_bytes / sizeof(std::seed_seq::result_type);
            auto seed = std::array<std::seed_seq::result_type, seed_len>();
            auto dev = std::random_device();
            std::generate_n(begin(seed), seed_len, std::ref(dev));
            auto seed_seq = std::seed_seq(begin(seed), end(seed));
            return T{seed_seq};
        }

        static auto getRndString(std::size_t const& len = 12) -> std::string {
            static constexpr auto chars =
                    "0123456789"
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                    "abcdefghijklmnopqrstuvwxyz";
            thread_local auto rng = random_generator<>();
            auto dist = std::uniform_int_distribution{{}, std::strlen(chars) - 1};
            auto result = std::string(len, '\0');
            std::generate_n(std::execution::par_unseq, begin(result), len, [&]() { return chars[dist(rng)]; });
            return result;
        }


        /**
         * Asynchronously connects to the gazelle server at the host and port specified.
         * @param host - Host of your gazelle server
         * @param subPort - Port of your gazelle server
         * @param pubPort - Port of your gazelle server
         * @param commPort - Port of your gazelle server
         */
        Client& connectToHub(char const* host = "localhost", int const subPort = 5875, int const pubPort = 5876, int const commPort = 5877) {
            if (!connectCalled) {
                connectCalled = true;
                std::string pubName{"p#"};
                pubName.append(getRndString(8));

                std::string subName{"s#"};
                subName.append(getRndString(8));

                std::string commName{"c#"};
                commName.append(getRndString(8));

                subClient.setOnReady([this]() {
                    isReady.fetch_add(1);
                    std::cout << "Subscriber connected to GazelleMQ" << std::endl;
                    if (isReady == 3 && onReadyFn != nullptr) {
                        onReadyFn();
                        onReadyFn = nullptr;
                    }
                });

                pubClient.setOnReady([this]() {
                    isReady.fetch_add(1);
                    std::cout << "Publisher connected to GazelleMQ" << std::endl;
                    if (isReady == 3 && onReadyFn != nullptr) {
                        onReadyFn();
                        onReadyFn = nullptr;
                    }
                });

                commandClient.setSubscriberName(subName);
                commandClient.setOnReady([this]() {
                    isReady.fetch_add(1);
                    std::cout << "Commander connected to GazelleMQ" << std::endl;
                    if (isReady == 3 && onReadyFn != nullptr) {
                        onReadyFn();
                        onReadyFn = nullptr;
                    }
                });

                subClient.connectToHub(subName.c_str(), host, subPort);
                pubClient.connectToHub(pubName.c_str(), host, pubPort);
                commandClient.connectToHub(commName.c_str(), host, commPort);
            }
            return *this;
        }

        /**
         * Publishes a message to the hub. Subscribers of [messageType] will receive the message.
         * @param messageType - The message type. Subscribers must subscribe to this exact string to handle the message.
         * @param messageContent - The message contents.
         */
        void publish(std::string&& messageType, std::string&& messageContent) {
            pubClient.publish(std::move(messageType), std::move(messageContent));
        }

        /**
         * Subscribes to a single message type.
         * @param messageTypeId - The message type ID
         * @param callback - Callback when an event is published that has the passed in [messageTypeId]
         * @param onAddedCallback - Callback to be called after the subscription is added
         */
        Client& subscribe(std::string&& messageTypeId, std::function<void(std::string&&)>&& callback, std::function<void()>&& onAddedCallback = nullptr) {
            commandClient.subscribe(messageTypeId, std::move(onAddedCallback));
            subClient.subscribe(std::move(messageTypeId), std::move(callback));
            return *this;
        }
    };

    static inline Client _client{};

    static Client& getClient() {
        return gazellemq::client::_client;
    }
}

#endif //GAZELLEMQ_CLIENT_CLIENT_HPP
