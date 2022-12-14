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
	HEAD,
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
	const std::string session_id;
	std::string username;

  public:
	Session(const Session &) = delete;
	Session &operator=(const Session &) = delete;
	Session(const std::string &sid) : session_id(sid) {}

	static std::pair<Session &, bool> get_session(const std::string &cookie);
	std::string get_username() const;
	void set_username(const std::string &username);
};

typedef std::unordered_multimap<std::string, std::string> Headers;
struct Response {
	Response(const std::string_view &params, Headers &&req_headers, std::istream &req_body);
	Response(const Response &) = delete;
	Response &operator=(const Response &) = delete;

	const Headers req_headers;
	std::istream &req_body;

	Headers resp_headers;
	Session &session;
	Status status = Status::OK;

	std::string get_username_api();
	void assert_logged_in();
	void assert_content_type(const std::string &content_type);
	std::unordered_map<std::string, std::string> get_params();
	std::unordered_map<std::string, std::string> parse_www_form();

  private:
	const std::string_view params;
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