#pragma once

#include <map>
#include <string>
#include <unordered_map>
#include <vector>
#include <cstring>

class KVstore {
	std::unordered_map<std::string, std::map<std::string, std::string, std::greater<std::string>>> kvstore;
	std::vector<bool> status = std::vector<bool>(6);

  public:
	std::string get(const std::string &rkey, const std::string &ckey) {
		const auto it = kvstore.find(rkey);
		if (it == kvstore.end()) {
			return "";
		}
		const auto it2 = it->second.find(ckey);
		if (it2 == it->second.end()) {
			return "";
		}
		return it2->second;
	}
	void put(const std::string &rkey, const std::string &ckey, const std::string &value) {
		if (value.empty()) {
			kvstore[rkey].erase(ckey);
		} else {
			kvstore[rkey][ckey] = value;
		}
	}
	bool cput(const std::string &rkey, const std::string &ckey, const std::string &value1, const std::string &value2) {
		auto &row = kvstore[rkey];
		const auto it = row.find(ckey);
		if (it == row.end()) {
			if (value1.empty()) {
				row[ckey] = value2;
				return true;
			}
			return false;
		}
		if (it->second == value1) {
			it->second = value2;
			return true;
		}
		return false;
	}

	void kill(int server_index) { status[server_index] = false; }
	void resurrect(int server_index) { status[server_index] = true; }
	std::vector<bool> list_server_status() { return status; }

	std::vector<std::string> list_rowkeys() {
		std::vector<std::string> ret;
		for (const auto &it : kvstore) {
			ret.emplace_back(it.first);
		}
		return ret;
	}
	std::vector<std::string> list_colkeys(const std::string &rkey) {
		std::vector<std::string> ret;
		const auto it = kvstore.find(rkey);
		if (it == kvstore.end()) {
			return ret;
		}
		for (const auto &it2 : it->second) {
			ret.emplace_back(it2.first);
		}
		return ret;
	}

	std::string storage_create(const std::string &rkey, const std::string &parent, const std::string &name,
							   bool is_dir) {
		std::string content = get(rkey, parent);
		if (content.empty()) {
			return "inode does not exist";
		}
		if (content[0] != '/') {
			return "inode is not a directory";
		}
		if (content.find("/" + name + ":") != std::string::npos) {
			return "file/dir already exists";
		}

		const std::string new_counter = std::to_string(std::stoi(get(rkey, "c")) + 1);
		put(rkey, "c", new_counter);

		content += name + ":" + new_counter + (is_dir ? "d/" : "f/");
		put(rkey, parent, content);

		if (is_dir) {
			put(rkey, new_counter, "/");
		}

		return new_counter;
	}

	void dele(const std::string &rkey, const std::string &ckey) { put(rkey, ckey, ""); }

	std::string storage_rename(const std::string &rkey, const std::string &parent, const std::string &name,
							   const std::string &target2) {
		std::string content = get(rkey, parent);
		if (content.empty()) {
			return "inode does not exist";
		}
		if (content[0] != '/') {
			return "inode is not a directory";
		}
		const std::size_t pos = content.find("/" + name + ":");
		if (pos == std::string::npos) {
			return "file/dir does not exist";
		}

		const std::size_t end = content.find("/", pos + 1);
		const std::size_t prefix_len = pos + name.size() + 2;
		std::string inode = content.substr(prefix_len, end + 1 - prefix_len);

		content.erase(pos + 1, end - pos);

		if (target2.empty()) {
			inode.pop_back();
			if (inode.back() == 'd') {
				inode.pop_back();
				const std::string content2 = get(rkey, inode);
				if (content2.size() > 1) {
					return "directory is not empty";
				}
			} else {
				inode.pop_back();
			}
			dele(rkey, inode);
			put(rkey, parent, content);
			return "";
		}

		// split target with '%2F'
		const std::size_t pos2 = target2.find("%2F");
		if (pos2 == std::string::npos) {
			return "invalid target";
		}
		const std::string target = target2.substr(0, pos2);
		const std::string ren = target2.substr(pos2 + std::strlen("%2F"));

		std::string content2;
		if (target == parent) {
			content2 = content;
		} else {
			content2 = get(rkey, target);
			if (content2.empty()) {
				return "target inode does not exist";
			}
			if (content2[0] != '/') {
				return "target inode is not a directory";
			}
		}

		if (content2.find("/" + ren + ":") != std::string::npos) {
			return "target file/dir already exists";
		}

		content2 += ren + ":" + inode;
		if (target != parent) {
			put(rkey, parent, content);
		}
		put(rkey, target, content2);
		return "";
	}
};

extern KVstore kvstore;