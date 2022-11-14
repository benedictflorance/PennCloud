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

	// 4xx
	static const Status NOT_FOUND;

	// 5xx
	static const Status HTTP_VERSION_NOT_SUPPORTED;
};

struct Response {
	const Method method;
	const std::string_view path;
	const std::istream &req_body;

	Status status = Status::OK;
	std::unique_ptr<std::istream> resp_body;
};

typedef std::function<void(Response &)> HandlerFunc;
void handle_socket(int sock);
void register_handler(const char *path, Method method, const HandlerFunc &handler);

} // namespace http