#pragma once

#include "../kvstore/client_wrapper.h"
#include "http.hpp"

#include <iomanip>
#include <map>
#include <memory>
#include <sstream>
#include <string>
// KVstore kvstore;

static std::unique_ptr<std::istream> get_file(http::Response &resp) {
	const std::string username = resp.get_username_api();

	const std::unordered_map<std::string, std::string> form = resp.get_params();
	const auto id = form.find("id");
	if (id == form.end() || id->second.empty()) {
		throw http::Exception(http::Status::BAD_REQUEST, "Missing file inode");
	}

	std::string content = kvstore.get("STORAGE_" + username, id->second);
	if (content.empty() || content.front() != 'f') {
		throw http::Exception(http::Status::BAD_REQUEST, "Invalid file inode");
	}
	content.erase(content.begin());
	return std::make_unique<std::istringstream>(content);
}

static std::unique_ptr<std::istream> list_dir(http::Response &resp) {
	const std::string username = resp.get_username_api();

	const std::unordered_map<std::string, std::string> form = resp.get_params();
	const auto id = form.find("id");
	if (id == form.end() || id->second.empty()) {
		throw http::Exception(http::Status::BAD_REQUEST, "Missing dir inode");
	}

	const std::string cont = kvstore.get("STORAGE_" + username, id->second);
	if (cont.empty() || cont.front() != '/') {
		throw http::Exception(http::Status::BAD_REQUEST, "Invalid dir inode");
	}
	const std::string_view content = cont;

	std::map<std::string_view, std::string_view> files, dir;

	std::size_t pos = 1;
	while (pos < content.size()) {
		const std::size_t next = content.find('/', pos);
		if (next == std::string_view::npos) {
		what:;
			throw http::Exception(http::Status::INTERNAL_SERVER_ERROR, "Inconsistent dir content!");
		}
		const std::string_view entry = content.substr(pos, next - pos);
		const std::size_t sep = entry.find(':');
		if (sep == std::string_view::npos) {
			goto what;
		}

		const std::string_view name = entry.substr(0, sep);
		const std::string_view inode = entry.substr(sep + 1, entry.size() - sep - 2);
		if (entry.back() == 'd') {
			dir.emplace(name, inode);
		} else {
			files.emplace(name, inode);
		}

		pos = next + 1;
	}

	std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();

	*ss << "[[";
	bool comma = false;
	for (const auto &de : dir) {
		if (comma) {
			*ss << ",";
		} else {
			comma = true;
		}
		*ss << '[' << std::quoted(de.first) << ',' << std::quoted(de.second) << ']';
	}
	*ss << "],[";
	comma = false;
	for (const auto &de : files) {
		if (comma) {
			*ss << ",";
		} else {
			comma = true;
		}
		*ss << '[' << std::quoted(de.first) << ',' << std::quoted(de.second) << ']';
	}
	*ss << "]]";

	resp.resp_headers.emplace("Content-Type", "application/json");
	return ss;
}

static std::unique_ptr<std::istream> create_storage(http::Response &resp) {
	const std::string username = resp.get_username_api();

	const std::unordered_map<std::string, std::string> form = resp.get_params();

	const auto parent = form.find("parent");
	if (parent == form.end() || parent->second.empty()) {
		throw http::Exception(http::Status::BAD_REQUEST, "Missing parent inode");
	}
	const auto name = form.find("name");
	if (name == form.end() || name->second.empty()) {
		throw http::Exception(http::Status::BAD_REQUEST, "Missing name");
	}
	const bool is_dir = form.count("dir");
	const std::string rkey = "STORAGE_" + username;

	const std::string ctr = kvstore.storage_create(rkey, parent->second, name->second, is_dir);
	if (ctr.empty() || !std::isdigit(ctr.front())) {
		throw http::Exception(http::Status::BAD_REQUEST, "Failed to create " + name->second + ": " + ctr);
	}
	if (!is_dir) {
		kvstore.put(rkey, ctr, "f" + resp.dump_body());
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
		std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();
		*ss << "Failed to ";
		if (target.empty()) {
			*ss << "delete ";
		} else {
			*ss << "rename ";
		}
		*ss << name->second << ": " << msg;
		resp.status = http::Status::BAD_REQUEST;
		return ss;
	}

	return nullptr;
}