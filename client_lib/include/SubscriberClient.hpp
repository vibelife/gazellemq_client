#ifndef GAZELLEMQ_CLIENT_SUBSCRIBERCLIENT_HPP
#define GAZELLEMQ_CLIENT_SUBSCRIBERCLIENT_HPP

#include <netdb.h>
#include <liburing.h>
#include <thread>
#include <condition_variable>
#include <vector>
#include <netinet/tcp.h>
#include <cstring>
#include <sys/epoll.h>
#include <functional>
#include "MPMCQueue/MPMCQueue.hpp"

namespace gazellemq::client {
    class SubscriberClient {
    private:
        enum ClientStep {
            ClientStep_NotSet,
            ClientStep_ConnectToHub,
            ClientStep_EPollSetup,
            ClientStep_SendingMessage,
            ClientStep_SendingIntent,
            ClientStep_SendingName,
            ClientStep_SendingSubscriptions,
            ClientStep_Ack,
            ClientStep_ReceiveData,
            ClientStep_Disconnect,
            ClientStep_Reconnect,
        };

        enum ParseState {
            ParseState_messageType,
            ParseState_messageContentLength,
            ParseState_messageContent,
        };

        static constexpr auto CHAR_LEN = sizeof(char);
        static constexpr auto BROKEN_PIPE = -32;
        static constexpr auto TIMEOUT = -62;
        static constexpr auto MAX_READ_BUF = 8192;
        static constexpr auto NB_INTENT_CHARS = 2;
        static constexpr addrinfo hints{0, AF_INET, SOCK_STREAM, 0, 0, nullptr, nullptr, nullptr};
        constexpr static auto SUBSCRIBER_INTENT = "S\r";


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


        struct gaicb* gai{};


        ParseState parseState{ParseState_messageType};

        ClientStep step{ClientStep_NotSet};

        std::string hubHost;
        int hubPort{};
        bool isConnected{false};
        bool wasConnectToHubCalled{false};

        io_uring ring{};
        int epfd{};
        int fd{};
        size_t const eventQueueDepth;
        size_t const maxEventBatch;
        std::jthread bgThread;

        std::string nextBatch;
        std::string writeBuffer;
        size_t nbNameBytesSent{};

        std::atomic_flag isRunning{true};

        rigtorp::MPMCQueue<std::string> queue;
        std::string clientName;

        std::string csvSubscriptions;
        size_t csvSubscriptionsIndex{};

        char readBuffer[MAX_READ_BUF]{};
        std::string message;
        char ackBuffer[1]{};

        std::string messageType;
        std::string messageLengthBuffer;
        std::string messageContent;
        size_t messageContentLength{};

        size_t nbMessageBytesRead{};
        size_t nbContentBytesRead{};

        rigtorp::MPMCQueue<Message> messages;
        std::vector<MessageHandler> handlers;
        std::vector<std::jthread> handlerThreads;
        int const nbMessageHandlerThreads;

        std::condition_variable cvMessageQueue;
        std::mutex mMessageQueue;
        std::atomic_flag hasMessages{false};


    public:
        explicit SubscriberClient(
                int const messagesQueueSize = 500000,
                int const maxEventBatch = 32,
                int const nbMessageHandlerThreads = 8,
                int const queueDepth = 8192,
                int const eventQueueDepth = 8192
        ) noexcept:
                messages(std::max(messagesQueueSize, 1)),
                maxEventBatch(std::max(maxEventBatch, 1)),
                nbMessageHandlerThreads(std::max(nbMessageHandlerThreads, 1)),
                queue(queueDepth),
                eventQueueDepth(eventQueueDepth)
        {}

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
        void connectToHub(char const* name, char const* host, int const port = 3822) {
            clientName.append(name);

            if (!wasConnectToHubCalled) {
                wasConnectToHubCalled = true;
                init();
                connectToServer(host, port);
            }
        }

        /**
         * Subscribes to a single message type.
         * @param messageTypeId - The message type ID
         * @param callback - Callback when an event is published that has the passed in [messageTypeId]
         */
        SubscriberClient& subscribe(std::string&& messageTypeId, std::function<void(std::string&&)>&& callback) {
            if (!csvSubscriptions.empty()) {
                csvSubscriptions.append(",");
            }

            csvSubscriptions.append(messageTypeId);

            handlers.emplace_back(MessageHandler::create(std::move(messageTypeId), std::move(callback)));

            return *this;
        }
    private:
        /**
         * Initialization code here
         */
        void init() {
            signal(SIGINT, sigintHandler);
            io_uring_queue_init(eventQueueDepth, &ring, 0);
            monitorMessageQueue();
        }

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
         * Asynchronously connects to the gazelle server at the host and port specified.
         * @param host - Host of your gazelle server
         * @param port - Port of your gazelle server
         */
        void connectToServer(char const* host, int const port) {
            hubHost = std::string{host};
            hubPort = port;

            bgThread = std::jthread([this]() {
                if (connect()) {
                    doEventLoop();
                }
            });
        }

        /**
         * Connects to the hub
         * @param socketFd
         * @param nextStep
         * @return
         */
        bool connect() {
            std::string strPort{std::to_string(hubPort)};
            gai = new gaicb{hubHost.c_str(), strPort.c_str(), &hints, nullptr};
            std::vector<gaicb*> hosts;
            hosts.emplace_back(gai);
            getaddrinfo_a(GAI_WAIT, hosts.data(), static_cast<int>(hosts.size()), nullptr);

            auto hi = this->gai;
            int ret = gai_error(hi);
            if (ret == 0) {
                if (hi->ar_result == nullptr) {
                    printf("Could not connect to %s:%d\n", hubHost.c_str(), hubPort);
                    clearGaiVector(std::move(hosts));
                    return false;
                }

                // try to create the socket
                for (addrinfo* rp = hi->ar_result; rp != nullptr; rp = rp->ai_next) {
                    fd = socket(rp->ai_family, rp->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC, rp->ai_protocol);

                    if (fd == -1) {
                        continue;
                    }

                    // https://news.ycombinator.com/item?id=10607422
                    // https://rigtorp.se/sockets/
                    int opt = 1;
                    size_t const optLen = sizeof(opt);

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


                    auto sqe = io_uring_get_sqe(&ring);
                    io_uring_prep_connect(sqe, fd, this->gai->ar_result->ai_addr, this->gai->ar_result->ai_addrlen);
                    // io_uring_sqe_set_data(sqe, this);

                    step = ClientStep_ConnectToHub;
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
         * Sets up epoll
         * @param res
         */
        void beginEPollSetup(int res) {
            if (res < 0) {
                printError("Could not connect to server, retrying...");
                // Cant continue in this state
                // exit(0);
            } else {
                printf("Connected to the hub\n");
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
                io_uring_submit(&ring);
            }
        }

        /**
         * Epoll has been set up. Next we send the intent.
         * @param res
         */
        void onEPollSetupComplete(int res) {
            if (res == 0) {
                writeBuffer.clear();
                writeBuffer.append(SUBSCRIBER_INTENT);

                beginSendIntent();
            } else {
                printError("epoll setup failed\n");
                // Cant continue in this state
                exit(0);
            }
        }

        /**
         * Submit a send data request
         */
        void beginSendIntent() {
            io_uring_sqe* sqe = io_uring_get_sqe(&ring);
            io_uring_prep_send(sqe, fd, writeBuffer.c_str(), writeBuffer.size(), 0);

            step = ClientStep_SendingIntent;
            io_uring_submit(&ring);
        }

        /**
         * At this point we are done communicating with the hub. Now messages can be published.
         * @param res
         */
        void onSendIntentComplete(int res) {
            writeBuffer.erase(0, res);
            if (writeBuffer.empty()) {
                writeBuffer.clear();
                writeBuffer.append(clientName);
                writeBuffer.append("\r");
                beginSendName();
            } else {
                beginSendIntent();
            }
        }

        /**
         * Sends the name to the hub
         */
        void beginSendName() {
            io_uring_sqe* sqe = io_uring_get_sqe(&ring);
            io_uring_prep_send(sqe, fd, writeBuffer.c_str(), writeBuffer.size(), 0);

            step = ClientStep_SendingName;
            io_uring_submit(&ring);
        }

        /**
         * Checks if we are done sending the name bytes
         * @param res
         * @return
         */
        void onSendNameComplete(int res) {
            writeBuffer.erase(0, res);
            if (!writeBuffer.empty()) {
                beginSendName();
            }

            beginReceiveAck();
        }

        /**
         * Receives acknowledgment from the server
         */
        void beginReceiveAck() {
            io_uring_sqe* sqe = io_uring_get_sqe(&ring);
            io_uring_prep_recv(sqe, fd, ackBuffer, 1, 0);

            step = ClientStep_Ack;
            io_uring_submit(&ring);
        }

        void onReceiveAckComplete(int res) {
            if (!csvSubscriptions.ends_with("\r")) {
                csvSubscriptions.append("\r");
            }

            csvSubscriptionsIndex = 0;
            beginSendSubscriptions();
        }

        /**
         * Sends the subscriptions to the hub
         */
        void beginSendSubscriptions() {
            io_uring_sqe* sqe = io_uring_get_sqe(&ring);
            io_uring_prep_send(sqe, fd, &csvSubscriptions.c_str()[csvSubscriptionsIndex], csvSubscriptions.size() - csvSubscriptionsIndex, 0);

            step = ClientStep_SendingSubscriptions;
            io_uring_submit(&ring);
        }

        void onSendSubscriptionsComplete(int res) {
            csvSubscriptionsIndex += res;
            if (csvSubscriptionsIndex < csvSubscriptions.size()) {
                beginSendSubscriptions();
            } else {
                beginReceiveData();
            }
        }

        /**
         * Receives data from the hub
         */
        void beginReceiveData() {
            io_uring_sqe* sqe = io_uring_get_sqe(&ring);
            io_uring_prep_recv(sqe, fd, readBuffer, MAX_READ_BUF, 0);

            step = ClientStep_ReceiveData;
            io_uring_submit(&ring);
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

        /**
         * Disconnects from the server
         * @param ring
         */
        void beginDisconnect() {
            io_uring_sqe* sqe = io_uring_get_sqe(&ring);
            io_uring_prep_close(sqe, fd);

            step = ClientStep_Disconnect;
            io_uring_submit(&ring);
        }

        void onDisconnectComplete() {
            isConnected = false;
        }

        void parseMessage(char const* buffer, size_t bufferLength) {
            for (size_t i{0}; i < bufferLength; ++i) {
                char ch{buffer[i]};
                if (parseState == ParseState_messageType) {
                    if (ch == '|') {
                        parseState = ParseState_messageContentLength;
                        continue;
                    } else {
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
                        parseState = ParseState_messageType;
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
        void monitorMessageQueue() {
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


        /**
         * Runs the event loop
         */
        void doEventLoop() {
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

                        switch (step) {
                            case ClientStep_ConnectToHub:
                                beginEPollSetup(res);
                                break;
                            case ClientStep_EPollSetup:
                                onEPollSetupComplete(res);
                                break;
                            case ClientStep_SendingIntent:
                                onSendIntentComplete(res);
                                break;
                            case ClientStep_SendingName:
                                onSendNameComplete(res);
                                break;
                            case ClientStep_Ack:
                                onReceiveAckComplete(res);
                                break;
                            case ClientStep_SendingSubscriptions:
                                onSendSubscriptionsComplete(res);
                                break;
                            case ClientStep_ReceiveData:
                                onReceiveDataComplete(res);
                                break;
                            case ClientStep_Disconnect:
                                onDisconnectComplete();
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


    static inline SubscriberClient _clientSubscriber{
        1000000,
        32,
        8,
        8192,
        8192
    };

    static SubscriberClient& getSubscriberClient() {
        return gazellemq::client::_clientSubscriber;
    }
}

#endif //GAZELLEMQ_CLIENT_SUBSCRIBERCLIENT_HPP
