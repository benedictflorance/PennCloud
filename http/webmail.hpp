#pragma once
#include "../kvstore/client_wrapper.h"

#include "http.hpp"
#include "util.hpp"

#include <iomanip>
#include <iostream>
#include <sstream>
#include <vector>
#include "deliver-mails.hpp"
static inline std::vector<std::string_view> split(const std::string_view &s) {
	std::vector<std::string_view> res;
	std::size_t start = 0;
	while (start < s.size()) {
		std::size_t end = s.find(' ', start);
		if (end == std::string_view::npos) {
			res.emplace_back(s.substr(start));
			break;
		}
		res.emplace_back(s.substr(start, end - start));
		start = end + 1;
	}
	return res;
}

static inline std::pair<std::string, std::string_view> split_email(const std::string_view &email) {
	const int at_index = email.find('@');
	if (email.length() < 3 || std::count(email.begin(), email.end(), '@') != 1 || at_index == email.length() - 1 ||
		at_index == 0 || at_index == std::string::npos) {
		throw http::Exception(http::Status::BAD_REQUEST, "Invalid email format " + std::string(email));
	} // Email should at least have letter@letter
	return std::make_pair(std::string(email.substr(0, at_index)), email.substr(at_index + 1));
}

static std::unique_ptr<std::istream> list_emails(http::Response &resp) {
	const std::vector<std::string> emails = kvstore.list_colkeys("MAILBOX_" + resp.get_username_api());
	resp.resp_headers.emplace("Content-Type", "application/json");
	return to_json(emails);
}

static std::unique_ptr<std::istream> get_email(http::Response &resp) {
	const std::string username = resp.get_username_api();

	const std::unordered_map<std::string, std::string> params = resp.get_params();
	const auto key = params.find("key");
	if (key == params.end() || key->second.empty()) {
		throw http::Exception(http::Status::BAD_REQUEST, "Missing key");
	}

	const std::string email_buf = kvstore.get("MAILBOX_" + username, key->second);
	if (email_buf.empty()) {
		throw http::Exception(http::Status::NOT_FOUND, "Email not found");
	}
	return std::make_unique<std::istringstream>(std::move(email_buf));
}

static std::unique_ptr<std::istream> send_email(http::Response &resp) {
	static unsigned short inc = 0;

	const std::string from = resp.get_username_api() + "@penncloud";

	const std::string bodyc = resp.dump_body();
	const std::string_view body = bodyc;

	const std::size_t pos = body.find('\n');
	if (pos == std::string::npos) {
		throw http::Exception(http::Status::BAD_REQUEST, "No subject");
	}

	const std::size_t pos2 = body.find('\n', pos + 1);
	if (pos2 == std::string::npos) {
		throw http::Exception(http::Status::BAD_REQUEST, "No emails");
	}
	const std::vector<std::string_view> email_vec = split(body.substr(pos + 1, pos2 - pos - 1));

	const std::time_t dt = std::time(nullptr);

	const std::string ckey =
		std::to_string(static_cast<uint64_t>(dt) * 1000 + inc++) + "/" + from + "/" + bodyc.substr(0, pos);
	if (inc == 1000)
		inc = 0;

	std::vector<std::pair<std::string, std::string_view>> to_emails;
	const std::string ser = bodyc.substr(pos2 + 1);

	// 1st pass: check if all users exist
	for (const auto &email : email_vec) {
		const auto p = split_email(email);
		if (p.second == "penncloud") {
			if (kvstore.get("ACCOUNT", p.first).empty()) {
				throw http::Exception(http::Status::BAD_REQUEST, "User " + p.first + " does not exist");
						to_emails.emplace_back(std::move(p));
			}
		} else {
			std::string subject = bodyc.substr(0, pos + 1);
			auto time_now = chrono::system_clock::to_time_t(chrono::system_clock::now());
			string date = string(ctime(&time_now));
			const std::string from_str = "From: User at PennCloud <" + from + ">\r\n";
			const std::string to_str = "To: Friend in External World <" + to + ">\r\n";
			const std::string date_str = "Date: Fri, 16 Dec 2022 16:40:11 -0400\r\n";
			const std::string subject_str = "Subject: " + subject + "\r\n\r\n";
			bool suceed = send_nonlocal_email(from, std::string(email), from_str + to_str + date_str + subject_str + ser);
			if(!suceed) {
				throw http::Exception(http::Status::BAD_REQUEST, "Email to " + std::string(email) + " failed to send");
			}

		}
	}

	// 2nd pass: send emails
	for (const auto &[to, domain] : to_emails) {
		std::string reconstruct = to + "@";
		reconstruct += domain;
		if (domain == "penncloud") {
			kvstore.put("MAILBOX_" + to, ckey, reconstruct + "\r\n" + ser);
		} else {
			// Construct headers
		}
	}

	return nullptr;
}

static std::unique_ptr<std::istream> delete_email(http::Response &resp) {
	const std::string username = resp.get_username_api();
	const std::string key = resp.dump_body();
	if (key.empty()) {
		throw http::Exception(http::Status::BAD_REQUEST, "No key");
	}
	kvstore.dele("MAILBOX_" + username, key);
	return nullptr;
}
