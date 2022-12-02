#include <string>
#include <unordered_map>

class KVstore {
	std::unordered_map<std::string, std::unordered_map<std::string, std::string>> kvstore;

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
};

extern KVstore kvstore;