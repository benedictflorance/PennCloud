#pragma once

#include "../kvstore/local_test.hpp"
#include <string>
KVstore kvstore;

static std::unique_ptr<std::istream> create_storage(http::Response &resp) {
	const std::string username = resp.get_username_api();

	const std::unordered_map<std::string, std::string> form = resp.parse_www_form();

	const auto parent = form.find("parent");
	if (parent == form.end() || parent->second.empty()) {
		throw http::Exception(http::Status::BAD_REQUEST, "Missing parent inode");
	}
	const auto name = form.find("name");
	if (name == form.end() || name->second.empty()) {
		throw http::Exception(http::Status::BAD_REQUEST, "Missing name");
	}
	const auto type = form.find("content");
	const std::string rkey = "STORAGE_" + username;

	const std::string ctr = kvstore.storage_create(rkey, parent->second, name->second, type == form.end());
	if (ctr.empty() || !std::isdigit(ctr.front())) {
		throw http::Exception(http::Status::BAD_REQUEST, "Failed to create file/dir: " + ctr);
	}
	if (type != form.end()) {
		kvstore.put(rkey, ctr, "f" + type->second);
	}

	return std::make_unique<std::istringstream>(ctr);
}

static std::unique_ptr<std::istream> rename_storage(http::Response &resp) {
	const std::string username = resp.get_username_api();

	const std::unordered_map<std::string, std::string> form = resp.parse_www_form();

	const auto parent = form.find("parent");
	if (parent == form.end() || parent->second.empty()) {
		throw http::Exception(http::Status::BAD_REQUEST, "Missing parent inode");
	}
	const auto name = form.find("name");
	if (name == form.end() || name->second.empty()) {
		throw http::Exception(http::Status::BAD_REQUEST, "Missing name");
	}
	std::string target;
	{
		const auto it = form.find("target");
		if (it != form.end())
			target = it->second;
	}

	const std::string msg = kvstore.storage_rename("STORAGE_" + username, parent->second, name->second, target);
	if (!msg.empty()) {
		throw http::Exception(http::Status::BAD_REQUEST, "Failed to rename file/dir: " + msg);
	}

	return nullptr;
}