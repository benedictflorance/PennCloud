#include "http.hpp"

#include <unistd.h>

#include <iostream>
#include <sstream>
#include <thread>

#include <ext/stdio_filebuf.h>
#include <netinet/in.h>
#include <sys/socket.h>

static std::unique_ptr<std::istream> index_page(http::Response &resp) {
	http::Session &session = resp.get_session();

	if (session.get_username().empty()) {
		resp.resp_headers.emplace("Content-Type", "text/html");
		return std::make_unique<std::ifstream>("static/login.html");
	}

	resp.resp_headers.emplace("Content-Type", "text/html");
	std::unique_ptr<std::stringstream> ss = std::make_unique<std::stringstream>();
	*ss << "Welcome, " << session.get_username() << "!";
	*ss << R"(<br /><a href="/logout">Logout</a>)";
	return ss;
}

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

	http::Session &session = resp.get_session();

	if (username == "admin" && password == "admin") {
		session.set_username(username);
		resp.resp_headers.emplace("Location", "/");
		resp.status = http::Status::FOUND;
		return nullptr;
	}
	throw http::Exception(http::Status::FORBIDDEN, "Invalid username or password");
}

static void handle(const int client) {
	try {
		http::handle_socket(client);
	} catch (const std::exception &e) {
		std::cerr << "Error: " << e.what() << std::endl;
	}
}

int main() {
	http::register_handler("/", http::Method::GET, index_page);
	http::register_handler("/login", http::Method::POST, login);
	http::register_handler("/logout", http::Method::GET, [](http::Response &resp) {
		resp.get_session().set_username("");
		resp.resp_headers.emplace("Location", "/");
		resp.status = http::Status::FOUND;
		return nullptr;
	});

	const int sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (sock == -1) {
		throw std::system_error(errno, std::generic_category(), "socket");
	}

	const sockaddr_in addr = {
		.sin_family = AF_INET,
		.sin_port = htons(12450),
		.sin_addr = {.s_addr = INADDR_ANY},
	};

	if (bind(sock, reinterpret_cast<const sockaddr *>(&addr), sizeof(addr)) == -1) {
		throw std::system_error(errno, std::generic_category(), "bind");
	}

	if (listen(sock, 10) == -1) {
		throw std::system_error(errno, std::generic_category(), "listen");
	}

	while (true) {
		const int client = accept(sock, nullptr, nullptr);
		if (client == -1) {
			throw std::system_error(errno, std::generic_category(), "accept");
		}

		std::thread(handle, client).detach();
	}
}