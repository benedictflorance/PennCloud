#pragma once
#include "../kvstore/client_wrapper.h"

#include "http.hpp"
#include "util.hpp"

#include "deliver-mails.hpp"
#include <iomanip>
#include <iostream>
#include <sstream>
#include <vector>
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

static inline std::string decodeURIComponent(const std::string &inp) {
	std::string res;
	res.reserve(inp.size());
	for (std::size_t i = 0; i < inp.size(); ++i) {
		if (inp[i] == '%' && i + 2 < inp.size()) {
			const char c1 = inp[i + 1];
			const char c2 = inp[i + 2];
			if (std::isxdigit(c1) && std::isxdigit(c2)) {
				const char c = (c1 <= '9' ? c1 - '0' : (c1 <= 'F' ? c1 - 'A' + 10 : c1 - 'a' + 10)) * 16 +
							   (c2 <= '9' ? c2 - '0' : (c2 <= 'F' ? c2 - 'A' + 10 : c2 - 'a' + 10));
				res.push_back(c);
				i += 2;
				continue;
			}
		}
		res.push_back(inp[i]);
	}
	return res;
}

static std::unique_ptr<std::istream> send_email(http::Response &resp) {
	static unsigned short inc = 0;

	const std::string username = resp.get_username_api();
	const std::string from = username + "@penncloud";

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

	const std::time_t dt = std::time(nullptr);
	const std::string subject = bodyc.substr(0, pos);

	const std::string ckey = std::to_string(static_cast<uint64_t>(dt) * 1000 + inc++) + "/" + from + "/" + subject;
	if (inc == 1000)
		inc = 0;

	const std::string ser = bodyc.substr(pos + 1);

	// external body
	std::ostringstream ebody;
	ebody << "From: " << username << " <" << from << ">\r\n";
	ebody << "To: ";

	// 1st pass: construct external body
	std::vector<std::pair<std::string, std::string_view>> to_emails;
	bool comma = false;
	for (const auto &email : split(body.substr(pos + 1, pos2 - pos - 1))) {
		const auto p = split_email(email);
		if (comma) {
			ebody << ", ";
		} else {
			comma = true;
		}
		ebody << p.first << " <" << email << ">";
		to_emails.emplace_back(std::move(p));
	}
	ebody << "\r\n";
	std::string date = ctime(&dt);
	date.pop_back();
	ebody << "Date: " << date << "\r\n";
	ebody << "Subject: " << decodeURIComponent(subject) << "\r\n";
	ebody << "\r\n";
	ebody << body.substr(pos2 + 1);

	// 2nd pass: check emails
	for (const auto &[to, domain] : to_emails) {
		if (domain == "penncloud") {
			if (kvstore.get("ACCOUNT", to).empty()) {
				throw http::Exception(http::Status::BAD_REQUEST, "User " + to + " does not exist");
			}
		}
	}

	const std::string ebody_s = ebody.str();
	// 3rd pass: send external emails
	for (const auto &[to, domain] : to_emails) {
		if (domain != "penncloud") {
			const std::string email = to + '@' + std::string(domain);
			bool succeed = send_nonlocal_email(from, email, ebody_s);
			if (!succeed) {
				throw http::Exception(http::Status::BAD_REQUEST, "Email to " + email + " failed to send");
			}
		}
	}

	// 4th pass: send internal emails
	for (const auto &[to, domain] : to_emails) {
		if (domain == "penncloud") {
			kvstore.put("MAILBOX_" + to, ckey, ser);
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
