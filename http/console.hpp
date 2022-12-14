#pragma once

#include "../kvstore/client_wrapper.h"
#include "http.hpp"

#include <iomanip>
#include <sstream>

static inline void to_json_val(std::ostream &s, const std::string &t) { s << std::quoted(t); }
static inline void to_json_val(std::ostream &s, bool t) { s << (t ? "true" : "false"); }

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

static std::unique_ptr<std::istream> change_status(http::Response &resp) {
	resp.get_username_api();
	const std::unordered_map<std::string, std::string> form = resp.parse_www_form();
	const auto it = form.find("id");
	if (it == form.end() || it->second.empty()) {
		throw http::Exception(http::Status::BAD_REQUEST, "Missing server index");
	}
	const auto it2 = form.find("status");
	if (it2 == form.end() || (it2->second != "1" && it2->second != "0")) {
		throw http::Exception(http::Status::BAD_REQUEST, "Missing server status");
	}
	const int index = std::stoi(it->second);
	if (it2->second == "1") {
		kvstore.resurrect(index);
	} else {
		kvstore.kill(index);
	}
	return nullptr;
}

static std::unique_ptr<std::istream> backend_status(http::Response &resp) {
	resp.get_username_api();
	resp.resp_headers.emplace("Content-Type", "application/json");
	return to_json(kvstore.list_server_status());
}

static std::unique_ptr<std::istream> raw_get(http::Response &resp) {
	resp.get_username_api();
	const std::unordered_map<std::string, std::string> form = resp.get_params();
	const auto rkey = form.find("rkey");
	if (rkey == form.end() || rkey->second.empty()) {
		throw http::Exception(http::Status::BAD_REQUEST, "Missing row key");
	}
	const auto ckey = form.find("ckey");
	if (ckey == form.end() || ckey->second.empty()) {
		throw http::Exception(http::Status::BAD_REQUEST, "Missing column key");
	}

	const std::string value = kvstore.get(rkey->second, ckey->second);
	if (value.empty()) {
		throw http::Exception(http::Status::NOT_FOUND, "Key not found");
	}
	return std::make_unique<std::stringstream>(value);
}

static std::unique_ptr<std::istream> list_rkey(http::Response &resp) {
	resp.get_username_api();
	resp.resp_headers.emplace("Content-Type", "application/json");
	return to_json(kvstore.list_rowkeys());
}

static std::unique_ptr<std::istream> list_ckey(http::Response &resp) {
	resp.get_username_api();
	const std::unordered_map<std::string, std::string> form = resp.get_params();
	const auto rkey = form.find("rkey");
	if (rkey == form.end() || rkey->second.empty()) {
		throw http::Exception(http::Status::BAD_REQUEST, "Missing row key");
	}

	resp.resp_headers.emplace("Content-Type", "application/json");
	return to_json(kvstore.list_colkeys(rkey->second));
}