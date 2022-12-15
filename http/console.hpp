#pragma once

#include "../kvstore/client_wrapper.h"
#include "http.hpp"
#include "util.hpp"

#include <atomic>
#include <iomanip>
#include <mutex>
#include <queue>
#include <sstream>

static inline void assert_admin(http::Response &resp) {
	if (resp.get_username_api() != "admin") {
		throw http::Exception(http::Status::FORBIDDEN, "Admin only");
	}
}

static std::unique_ptr<std::istream> change_status(http::Response &resp) {
	assert_admin(resp);
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
	assert_admin(resp);
	resp.resp_headers.emplace("Content-Type", "application/json");
	return to_json(kvstore.list_server_status());
}

static std::unique_ptr<std::istream> raw_get(http::Response &resp) {
	assert_admin(resp);
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
	assert_admin(resp);
	resp.resp_headers.emplace("Content-Type", "application/json");
	return to_json(kvstore.list_rowkeys());
}

static std::unique_ptr<std::istream> list_ckey(http::Response &resp) {
	assert_admin(resp);
	const std::unordered_map<std::string, std::string> form = resp.get_params();
	const auto rkey = form.find("rkey");
	if (rkey == form.end() || rkey->second.empty()) {
		throw http::Exception(http::Status::BAD_REQUEST, "Missing row key");
	}

	resp.resp_headers.emplace("Content-Type", "application/json");
	return to_json(kvstore.list_colkeys(rkey->second));
}

static class Info {
	std::mutex lock, lockc;
	std::atomic<int> client = -1;

	std::uint16_t online;
	std::queue<std::time_t> q;

	std::uint16_t count() {
		std::lock_guard<std::mutex> guard(lockc);
		const std::time_t now = std::time(nullptr);
		while (!q.empty() && q.front() + 3 < now) {
			q.pop();
		}
		return online + q.size();
	}

  public:
	void log_client(bool start) {
		std::lock_guard<std::mutex> guard(lockc);
		if (start) {
			++online;
		} else {
			--online;
			q.push(std::time(nullptr));
		}
	}

	void balancer(const int port) {
		const int sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
		if (sock == -1) {
			throw std::system_error(errno, std::generic_category(), "socket");
		}

		const sockaddr_in addr = {
			.sin_family = AF_INET,
			.sin_port = htons(port),
			.sin_addr = {.s_addr = INADDR_ANY},
		};

		if (bind(sock, reinterpret_cast<const sockaddr *>(&addr), sizeof(addr)) == -1) {
			throw std::system_error(errno, std::generic_category(), "bind");
		}

		if (listen(sock, 10) == -1) {
			throw std::system_error(errno, std::generic_category(), "listen");
		}

		while (true) {
			const int c = accept(sock, nullptr, nullptr);
			if (c == -1) {
				throw std::system_error(errno, std::generic_category(), "accept");
			}

			char buf[3];
			client = c;
			while (true) {
				buf[0] = '0';
				*(reinterpret_cast<uint16_t *>(buf + 1)) = htons(count());
				if (write(c, buf, 3) != 3)
					break;
				std::this_thread::sleep_for(std::chrono::seconds(1));
			}
			const std::lock_guard<std::mutex> _(lock);
			close(c);
			client = -1;
		}
	}

	std::vector<uint16_t> get_status() {
		const int c = client;
		if (c == -1)
			return {};

		const std::lock_guard<std::mutex> _(lock);
		if (write(c, "1", 1) != 1)
			return {};
		char buf[2];
		if (read(c, buf, 2) != 2)
			return {};
		std::vector<uint16_t> ret(ntohs(*(reinterpret_cast<uint16_t *>(buf))));
		for (uint16_t &i : ret) {
			if (read(c, buf, 2) != 2)
				return {};
			i = ntohs(*(reinterpret_cast<uint16_t *>(buf)));
		}
		return ret;
	}
} master_info;

static std::unique_ptr<std::istream> health_status(http::Response &resp) {
	assert_admin(resp);
	resp.resp_headers.emplace("Content-Type", "application/json");

	const std::vector<uint16_t> status = master_info.get_status();
	if (status.empty()) {
		throw http::Exception(http::Status::INTERNAL_SERVER_ERROR, "Load balancer not available");
	}

	return to_json(status);
}