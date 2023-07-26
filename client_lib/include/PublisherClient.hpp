#ifndef GAZELLE_CLIENT_PUBLISHERCLIENT2_HPP
#define GAZELLE_CLIENT_PUBLISHERCLIENT2_HPP

#include <netdb.h>
#include <liburing.h>
#include <thread>
#include <condition_variable>
#include <vector>
#include <netinet/tcp.h>
#include <cstring>
#include <sys/epoll.h>
#include "MPMCQueue/MPMCQueue.hpp"

namespace gazellemq::client {
    class PublisherClient {
    private:
        enum ClientStep {
            ClientStep_NotSet,
            ClientStep_ConnectToHub,
            ClientStep_EPollSetup,
            ClientStep_SendingMessage,
            ClientStep_SendingIntent,
            ClientStep_Reconnect,
        };

        static constexpr auto BROKEN_PIPE = -32;
        static constexpr auto TIMEOUT = -62;
        static constexpr auto MAX_READ_BUF = 8;
        static constexpr auto NB_INTENT_CHARS = 2;
        static constexpr addrinfo hints{0, AF_INET, SOCK_STREAM, 0, 0, nullptr, nullptr, nullptr};
        constexpr static auto PUBLISHER_INTENT = "P\r";

        struct gaicb* gai{};

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

        std::mutex mQueue;
        std::condition_variable cvQueue;
        std::atomic_flag hasPendingData{false};
        std::atomic_flag isRunning{true};

        int const messageBatchSize;
        rigtorp::MPMCQueue<std::string> queue;
        std::vector<int> raisedMessageTypeIds;


    public:
        explicit PublisherClient(
                int const queueDepth = 65536,
                int const eventQueueDepth = 8192,
                int const maxEventBatch = 32,
                int const msgBatchSize = 1
        ) noexcept:
                queue(queueDepth),
                eventQueueDepth(eventQueueDepth),
                maxEventBatch(maxEventBatch),
                messageBatchSize(std::max(msgBatchSize, 1))
        {
            writeBuffer.reserve(256);
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
        void connectToHub(char const* host, int const port = 3822) {
            if (!wasConnectToHubCalled) {
                wasConnectToHubCalled = true;
                init();
                connectToServer(host, port);
            }
        }

    private:
        /**
         * Initialization code here
         */
        void init() {
            signal(SIGINT, sigintHandler);
            io_uring_queue_init(eventQueueDepth, &ring, 0);
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
         * Alerts the thread that there is pending data
         */
        void notify() {
            if (!hasPendingData.test()) {
                std::lock_guard lock{mQueue};
                hasPendingData.test_and_set();
                cvQueue.notify_one();
            }
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
        bool beginEPollSetup(int res) {
            if (res < 0) {
                printError("Could not connect to server");
                // return beginWriteToBacklog();
                return false;
            } else {
                printf("Connected to the hub\n");
                isConnected = true;

                epfd = epoll_create1(0);
                if (epfd < 0) {
                    printError(strerror(-epfd));
                    return false;
                }

                io_uring_sqe* sqe = io_uring_get_sqe(&ring);
                struct epoll_event ev{};
                ev.events = EPOLLIN | EPOLLOUT | EPOLLHUP | EPOLLERR | EPOLLRDHUP;
                ev.data.fd = fd;
                io_uring_prep_epoll_ctl(sqe, epfd, fd, EPOLL_CTL_ADD, &ev);

                step = ClientStep_EPollSetup;
                io_uring_submit(&ring);

                return true;
            }
        }

        /**
         * Epoll has been set up. Next we send the intent.
         * @param res
         */
        bool onEPollSetupComplete(int res) {
            if (res == 0) {
                writeBuffer.clear();
                writeBuffer.append(PUBLISHER_INTENT);

                beginSendIntent();
                return true;
            } else {
                return false;
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
        bool onSendIntentComplete(int res) {
            if (res == NB_INTENT_CHARS) {
                return true;
            }
            return false;
        }

        /**
         * Sends data to the hub, or into the backlog file if necessary.
         */
        void beginSendData() {
            io_uring_sqe* sqe = io_uring_get_sqe(&ring);
            io_uring_prep_send(sqe, fd, writeBuffer.c_str(), writeBuffer.size(), 0);

            step = ClientStep_SendingMessage;
            io_uring_submit(&ring);
        }

        /**
         * Checks if more bytes need to be sent. Returns true if there is more sending to be done, false otherwise.
         * @param res
         */
        bool onDataSent(int res) {
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

            writeBuffer.append(nextBatch);

            if (!writeBuffer.empty()) {
                beginSendData();
                return true;
            }


            return false;
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

            outer:
            while (isRunning.test()) {
                if (step != ClientStep_NotSet && step != ClientStep_ConnectToHub) {
                    std::unique_lock uniqueLock{mQueue};
                    bool didTimeout{!cvQueue.wait_for(uniqueLock, 2s, [this]() { return hasPendingData.test(); })};

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

                            switch (step) {
                                case ClientStep_ConnectToHub:
                                    if (beginEPollSetup(res)) {
                                        break;
                                    } else {
                                        io_uring_cqe_seen(&ring, cqe);
                                        goto outer;
                                    }
                                case ClientStep_EPollSetup:
                                    if (onEPollSetupComplete(res)) {
                                        break;
                                    } else {
                                        io_uring_cqe_seen(&ring, cqe);
                                        goto outer;
                                    }
                                case ClientStep_SendingIntent:
                                    if (onSendIntentComplete(res)) {
                                        break;
                                    } else {
                                        io_uring_cqe_seen(&ring, cqe);
                                        goto outer;
                                    }
                                case ClientStep_SendingMessage:
                                    if (onDataSent(res)) {
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


    inline PublisherClient _clientPublisher{};

    static PublisherClient& getPublisherClient() {
        return gazellemq::client::_clientPublisher;
    }
}

#endif //GAZELLE_CLIENT_PUBLISHERCLIENT2_HPP
