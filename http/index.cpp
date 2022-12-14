#include "http.hpp"

#include <unistd.h>

#include <cstring>
#include <iostream>
#include <sstream>
#include <thread>

#include <ext/stdio_filebuf.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "console.hpp"
#include "account.hpp"
#include "storage.hpp"

static std::unique_ptr<std::istream> index_page(http::Response &resp) {
	resp.resp_headers.emplace("Content-Type", "text/html");
	const std::string &username = resp.session.get_username();
	if (username.empty()) {
		return std::make_unique<std::ifstream>("static/login.html");
	}
	std::string str;
	{
		std::ifstream t("static/index.html");
		str.assign((std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());
	}
	str.replace(str.find("{{username}}"), std::strlen("{{username}}"), username);
	return std::make_unique<std::istringstream>(str);
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
	http::register_handler("/mail", http::Method::GET, [](http::Response &resp) {
		resp.assert_logged_in();
		resp.resp_headers.emplace("Content-Type", "text/html");
		return std::make_unique<std::ifstream>("static/mail.html");
	});
	http::register_handler("/console", http::Method::GET, [](http::Response &resp) {
		resp.assert_logged_in();
		resp.resp_headers.emplace("Content-Type", "text/html");
		return std::make_unique<std::ifstream>("static/console.html");
	});

	http::register_handler("/kvstore/get", http::Method::GET, raw_get);
	http::register_handler("/kvstore/listr", http::Method::GET, list_rkey);
	http::register_handler("/kvstore/listc", http::Method::GET, list_ckey);

	http::register_handler("/bstatus/change", http::Method::POST, change_status);
	http::register_handler("/bstatus", http::Method::GET, backend_status);

	http::register_handler("/signup", http::Method::POST, signup);
	http::register_handler("/login", http::Method::POST, login);
	http::register_handler("/logout", http::Method::GET, logout);
	http::register_handler("/change", http::Method::POST, change_pass);

	http::register_handler("/storage/create", http::Method::POST, create_storage);
	http::register_handler("/storage/rename", http::Method::POST, rename_storage);

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

	std::cerr << "Listening on port 12450" << std::endl;
	while (true) {
		const int client = accept(sock, nullptr, nullptr);
		if (client == -1) {
			throw std::system_error(errno, std::generic_category(), "accept");
		}

		std::thread(handle, client).detach();
	}
}