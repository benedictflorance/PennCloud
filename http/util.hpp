#pragma once

#include "../kvstore/client_wrapper.h"

#include <iomanip>
#include <memory>
#include <sstream>

// KVstore kvstore;

static inline void to_json_val(std::ostream &s, const std::string &t) { s << std::quoted(t); }
static inline void to_json_val(std::ostream &s, bool t) { s << (t ? "true" : "false"); }
static inline void to_json_val(std::ostream &s, uint16_t t) { s << t; }

template <typename T> static inline std::unique_ptr<std::istream> to_json(const std::vector<T> &t) {
	std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();
	*ss << "[";
	bool comma = false;
	for (const auto &i : t) {
		if (comma) {
			*ss << ",";
		} else {
			comma = true;
		}
		to_json_val(*ss, i);
	}
	*ss << "]";
	return ss;
}