#ifndef GAZELLEMQ_CLIENT_RANDOM_HPP
#define GAZELLEMQ_CLIENT_RANDOM_HPP
#include <algorithm>
#include <array>
#include <cstring>
#include <functional>
#include <random>
#include <string>
#include <execution>

namespace vl::random {
    template <typename T = std::mt19937> static auto random_generator() -> T {
        auto constexpr seed_bytes = sizeof(typename T::result_type) * T::state_size;
        auto constexpr seed_len = seed_bytes / sizeof(std::seed_seq::result_type);
        auto seed = std::array<std::seed_seq::result_type, seed_len>();
        auto dev = std::random_device();
        std::generate_n(begin(seed), seed_len, std::ref(dev));
        auto seed_seq = std::seed_seq(begin(seed), end(seed));
        return T{seed_seq};
    }

    static auto getString(std::size_t const& len = 12) -> std::string {
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
};

#endif //GAZELLEMQ_CLIENT_RANDOM_HPP
