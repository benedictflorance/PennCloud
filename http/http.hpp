#pragma once

#include <functional>
#include <istream>
#include <memory>
#include <thread>
#include <unordered_map>

namespace http {
enum class Method {
	GET,
	POST,
};

struct Status : std::string_view {
	Status(const char *s) : std::string_view(s) {}

	// 2xx
	static const Status OK;

	// 3xx
	static const Status FOUND;

	// 4xx
	static const Status BAD_REQUEST;
	static const Status FORBIDDEN;
	static const Status NOT_FOUND;

	// 5xx
	static const Status HTTP_VERSION_NOT_SUPPORTED;
};

class Session {
	static std::unordered_map<std::string, Session> sessions;
	
	const std::string session_id;
	std::string username;

  public:
	Session(const Session &) = delete;
	Session &operator=(const Session &) = delete;
	Session(const std::string &sid) : session_id(sid) {}

	static std::pair<Session &, bool> get_session(const std::string &cookie);
	const std::string &get_username() const;
	void set_username(const std::string &username);
};

typedef std::unordered_multimap<std::string, std::string> Headers;
struct Response {
	const Method method;
	const std::string_view path;
	const Headers req_headers;
	std::istream &req_body;
	std::unordered_map<std::string, std::string> params;
	Status status = Status::OK;
	Headers resp_headers;

	Session &get_session();
	std::unordered_map<std::string, std::string> get_params();
	std::unordered_map<std::string, std::string> parse_www_form();
	std::unordered_map<std::string, std::string> parse_file_upload();
};

struct Exception : public std::exception {
	const Status status;
	const std::string message;
	Exception(const Status &s, const std::string &m = "") : status(s), message(m) {}
};

typedef std::function<std::unique_ptr<std::istream>(Response &)> HandlerFunc;
void handle_socket(int sock);
void register_handler(const char *path, Method method, const HandlerFunc &handler);

} // namespace http