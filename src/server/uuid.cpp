#include "synxpo/server/uuid.h"

#include <iomanip>
#include <random>
#include <sstream>

namespace synxpo::server {

std::string GenerateUuid() {
    static std::random_device rd;
    static std::mt19937_64 gen(rd());
    static std::uniform_int_distribution<uint64_t> dis;
    
    uint64_t high = dis(gen);
    uint64_t low = dis(gen);
    
    // Set version 4 (random)
    high = (high & 0xFFFFFFFFFFFF0FFFULL) | 0x0000000000004000ULL;
    // Set variant (RFC 4122)
    low = (low & 0x3FFFFFFFFFFFFFFFULL) | 0x8000000000000000ULL;
    
    std::ostringstream oss;
    oss << std::hex << std::setfill('0')
        << std::setw(8) << (high >> 32)
        << '-'
        << std::setw(4) << ((high >> 16) & 0xFFFF)
        << '-'
        << std::setw(4) << (high & 0xFFFF)
        << '-'
        << std::setw(4) << (low >> 48)
        << '-'
        << std::setw(12) << (low & 0xFFFFFFFFFFFFULL);
    
    return oss.str();
}

}  // namespace synxpo::server
