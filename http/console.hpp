#pragma once

#include "../kvstore/client_wrapper.h"
#include "http.hpp"

#include <sstream>

static std::unique_ptr<std::istream> change_status(http::Response &resp) {
	if (resp.session.get_username().empty()) {
		throw http::Exception(http::Status::FORBIDDEN, "Not logged in");
	}
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
    if (resp.session.get_username().empty()) {
        throw http::Exception(http::Status::FORBIDDEN, "Not logged in");
    }

    std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();
    *ss << '[';

    bool comma = false;
    for (const auto &&b : kvstore.list_server_status()) {
        if (comma) {
            *ss << ',';
        } else {
            comma = true;
        }
        *ss << (b ? "true" : "false");
    }
    *ss << ']';

    resp.resp_headers.emplace("Content-Type", "application/json");
    return ss;
}