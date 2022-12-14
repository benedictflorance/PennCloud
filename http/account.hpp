#pragma once

#include "../kvstore/local_test.hpp"
#include "http.hpp"

static std::unique_ptr<std::istream> login(http::Response &resp) {
	const std::unordered_map<std::string, std::string> form = resp.parse_www_form();
	std::string username, password;
	{
		const auto it = form.find("username");
		if (it == form.end() || it->second.empty()) {
			throw http::Exception(http::Status::BAD_REQUEST, "Missing username");
		}
		username = std::move(it->second);
	}

	{
		const auto it = form.find("password");
		if (it == form.end() || it->second.empty()) {
			throw http::Exception(http::Status::BAD_REQUEST, "Missing password");
		}
		password = std::move(it->second);
	}

	if (kvstore.get("ACCOUNT", username) == password) {
		resp.session.set_username(username);
		return nullptr;
	}
	throw http::Exception(http::Status::FORBIDDEN, "Invalid username or password");
}

static std::unique_ptr<std::istream> signup(http::Response &resp) {
	const std::unordered_map<std::string, std::string> form = resp.parse_www_form();
	std::string username, password;
	{
		const auto it = form.find("username");
		if (it == form.end() || it->second.empty()) {
			throw http::Exception(http::Status::BAD_REQUEST, "Missing username");
		}
		username = std::move(it->second);
	}

	{
		const auto it = form.find("password");
		if (it == form.end() || it->second.empty()) {
			throw http::Exception(http::Status::BAD_REQUEST, "Missing password");
		}
		password = std::move(it->second);
	}

	if (kvstore.cput("ACCOUNT", username, "", password)) {
		kvstore.put("STORAGE_" + username, "c", "0");
		kvstore.put("STORAGE_" + username, "0", "/");
		resp.session.set_username(username);
		return nullptr;
	}

	throw http::Exception(http::Status::FORBIDDEN, "User already exists");
}