#pragma once

#include "../kvstore/client_wrapper.h"
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
		kvstore.put("MAILBOX_" + username, "", "");
		resp.session.set_username(username);
		return nullptr;
	}

	throw http::Exception(http::Status::FORBIDDEN, "User already exists");
}

static std::unique_ptr<std::istream> logout(http::Response &resp) {
	resp.session.set_username("");
	resp.resp_headers.emplace("Location", "/");
	resp.status = http::Status::FOUND;
	return nullptr;
}

static std::unique_ptr<std::istream> change_pass(http::Response &resp) {
	const std::string username = resp.get_username_api();
	const std::unordered_map<std::string, std::string> form = resp.parse_www_form();

	std::string old_pass, new_pass;
	{
		const auto it = form.find("old");
		if (it == form.end() || it->second.empty()) {
			throw http::Exception(http::Status::BAD_REQUEST, "Missing old password");
		}
		old_pass = std::move(it->second);
	}

	{
		const auto it = form.find("new");
		if (it == form.end() || it->second.empty()) {
			throw http::Exception(http::Status::BAD_REQUEST, "Missing new password");
		}
		new_pass = std::move(it->second);
	}

	if (kvstore.cput("ACCOUNT", username, old_pass, new_pass)) {
		return nullptr;
	}

	throw http::Exception(http::Status::FORBIDDEN, "Incorrect password");
}