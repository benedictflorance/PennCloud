void create_nonexistent_mutex(string rowkey);
void process_get_request(PennCloud::Request &request, PennCloud::Response &response);
void process_put_request(PennCloud::Request &request, PennCloud::Response &response);
void process_cput_request(PennCloud::Request &request, PennCloud::Response &response);
void process_delete_request(PennCloud::Request &request, PennCloud::Response &response);

bool is_rowkey_accepted(string rowkey)
{
    int start_letter = rowkey[0];
    for(int i = 0; i < rowkey_range.size(); i++)
    {
        if(rowkey[0] >= toRowKeyRange(rowkey_range[i]).first && 
            rowkey[0] <= toRowKeyRange(rowkey_range[i]).second)
            return true;
    }
    return false;
}
void process_get_request(PennCloud::Request &request, PennCloud::Response &response)
{
    if(!request.has_rowkey() || !request.has_columnkey())
    {
        response.set_status(param_unset_message.first);
        response.set_description(param_unset_message.second);
    }
    else if(!is_rowkey_accepted(request.rowkey()))
    {
        response.set_status(invalid_rowkey_message.first);
        response.set_description(invalid_rowkey_message.second);
    }
    else
    {
        rowkey_lock[request.rowkey()].lock();
        if(kv_store.find(request.rowkey()) != kv_store.end())
        {
            if(kv_store[request.rowkey()].find(request.columnkey()) != kv_store[request.rowkey()].end())
            {
                response.set_status("+OK");
                response.set_value(kv_store[request.rowkey()][request.columnkey()]);
            }
            else
            {
                response.set_status(key_inexistence_message.first);
                response.set_description(key_inexistence_message.second);
            }
        }
        else
        {
            response.set_status(key_inexistence_message.first);
            response.set_description(key_inexistence_message.second);
        }
        rowkey_lock[request.rowkey()].unlock();
    }
}

void process_listcolkey_request(PennCloud::Request &request, PennCloud::Response &response)
{
    if(!request.has_rowkey())
    {
        response.set_status(param_unset_message.first);
        response.set_description(param_unset_message.second);
    }
    else if(!is_rowkey_accepted(request.rowkey()))
    {
        response.set_status(invalid_rowkey_message.first);
        response.set_description(invalid_rowkey_message.second);
    }
    else
    {
        rowkey_lock[request.rowkey()].lock();
        if(kv_store.find(request.rowkey()) != kv_store.end())
        {
            for(auto itr: kv_store[request.rowkey()]){
                response.add_col_keys(itr.first.c_str());            
            }

            response.set_status("+OK");
        }
        else
        {
            response.set_status(key_inexistence_message.first);
            response.set_description(key_inexistence_message.second);
        }
        rowkey_lock[request.rowkey()].unlock();
    }
}

void process_listrowkey_request(PennCloud::Request &request, PennCloud::Response &response)
{
    for(auto itr: kv_store){
        response.add_row_keys(itr.first.c_str());            
    }
    response.set_status("+OK");
}

void create_nonexistent_mutex(string rowkey)
{
    //create rowkey lock if it doesn't exist
    if(rowkey_lock.find(rowkey) == rowkey_lock.end()){
    	mutex lockrowkey;
    	rowkeymaplock.lock();
        // just use the operator[] - it will create the value using its default constructor and 
        // return a reference to it. Or it will just find and return a reference to the already 
        // existing item without creating a new one.
    	//rowkey_lock.insert({rowkey, lockrowkey});
    	rowkeymaplock.unlock();
    }
}
void process_put_request(PennCloud::Request &request, PennCloud::Response &response)
{
    if(!request.has_rowkey() || !request.has_columnkey() || !request.has_value1())
    {
        response.set_status(param_unset_message.first);
        response.set_description(param_unset_message.second);
    }
    else if(!is_rowkey_accepted(request.rowkey()))
    {
        response.set_status(invalid_rowkey_message.first);
        response.set_description(invalid_rowkey_message.second);
    }
    else
    {
        //create_nonexistent_mutex(request.rowkey());
        rowkey_lock[request.rowkey()].lock();
        if(kv_store.find(request.rowkey()) != kv_store.end())
        {
            kv_store[request.rowkey()][request.columnkey()] = request.value1();              
            response.set_status("+OK");
        }
        else
        {
            kv_store[request.rowkey()] = map<string, string, greater<string> >();
            kv_store[request.rowkey()][request.columnkey()] = request.value1();   
            response.set_status("+OK");
        }
        rowkey_lock[request.rowkey()].unlock();
    }
}
void process_cput_request(PennCloud::Request &request, PennCloud::Response &response)
{
    if(!request.has_rowkey() || !request.has_columnkey() || !request.has_value1() || !request.has_value2())
    {
        response.set_status(param_unset_message.first);
        response.set_description(param_unset_message.second);
    }
    else if(!is_rowkey_accepted(request.rowkey()))
    {
        response.set_status(invalid_rowkey_message.first);
        response.set_description(invalid_rowkey_message.second);
    }
    else
    {
        //create_nonexistent_mutex(request.rowkey());
        rowkey_lock[request.rowkey()].lock();
        if(kv_store.find(request.rowkey()) != kv_store.end())
        {
            if(kv_store[request.rowkey()].find(request.columnkey()) != kv_store[request.rowkey()].end())
            {
                if(kv_store[request.rowkey()][request.columnkey()] == request.value1())
                    kv_store[request.rowkey()][request.columnkey()] = request.value2();              
                response.set_status("+OK");
            }
            else
            {
                response.set_status(key_inexistence_message.first);
                response.set_description(key_inexistence_message.second);
            }
        }
        else
        {
            response.set_status(key_inexistence_message.first);
            response.set_description(key_inexistence_message.second);
        }
        rowkey_lock[request.rowkey()].unlock();
    }
}
void process_delete_request(PennCloud::Request &request, PennCloud::Response &response)
{
    if(!request.has_rowkey() || !request.has_columnkey())
    {
        response.set_status(param_unset_message.first);
        response.set_description(param_unset_message.second);
    }
    else if(!is_rowkey_accepted(request.rowkey()))
    {
        response.set_status(invalid_rowkey_message.first);
        response.set_description(invalid_rowkey_message.second);
    }
    else
    {
        //create_nonexistent_mutex(request.rowkey());
        rowkey_lock[request.rowkey()].lock();
        if(kv_store.find(request.rowkey()) != kv_store.end())
        {
            if(kv_store[request.rowkey()].find(request.columnkey()) != kv_store[request.rowkey()].end())
            {
                kv_store[request.rowkey()].erase(request.columnkey());
                response.set_status("+OK");
            }
            else
            {
                response.set_status(key_inexistence_message.first);
                response.set_description(key_inexistence_message.second);
            }
        }
        else
        {
            response.set_status(key_inexistence_message.first);
            response.set_description(key_inexistence_message.second);
        }
        rowkey_lock[request.rowkey()].unlock();
    }
}

void process_create_request(PennCloud::Request &request, PennCloud::Response &response)
{
    if(!request.has_rowkey() || !request.has_columnkey() || !request.has_value1() || !request.has_value2())
    {
        response.set_status(param_unset_message.first);
        response.set_description(param_unset_message.second);
    }
    else if(!is_rowkey_accepted(request.rowkey()))
    {
        response.set_status(invalid_rowkey_message.first);
        response.set_description(invalid_rowkey_message.second);
    }
    else
    {
        rowkey_lock[request.rowkey()].lock();
        if(kv_store.find(request.rowkey()) != kv_store.end())
        {
            if(kv_store[request.rowkey()].find(request.columnkey()) != kv_store[request.rowkey()].end())
            {
                string content = kv_store[request.rowkey()][request.columnkey()];
                if (content[0] != '/') 
                {
                    response.set_status(inode_notdir_message.first);
                    response.set_description(inode_notdir_message.second);
                }
                else if (content.find("/" + request.value1() + ":") != std::string::npos) 
                {
                    response.set_status(inode_exists_message.first);
                    response.set_description(inode_exists_message.second);
                }
                else
                {
                    std::string new_counter;
                    if(kv_store[request.rowkey()].find("c") != kv_store[request.rowkey()].end())
                        new_counter = std::to_string(std::stoi(kv_store[request.rowkey()]["c"]) + 1);
                    else
                        new_counter = "1";
                    kv_store[request.rowkey()]["c"] = new_counter;
                    bool is_dir = (bool) stoi(request.value2());
                    content += request.value1() + ":" + new_counter + (is_dir ? "d/" : "f/");
                    kv_store[request.rowkey()][request.columnkey()] = content;
                    if (is_dir) 
                    {
                        kv_store[request.rowkey()][new_counter] = "/";
                    }
                    response.set_status("+OK");
                    response.set_value(new_counter);
                }
            }
            else
            {
                response.set_status(inode_inexistence_message.first);
                response.set_description(inode_inexistence_message.second);
            }
        }
        else
        {
            response.set_status(inode_inexistence_message.first);
            response.set_description(inode_inexistence_message.second);
        }
        rowkey_lock[request.rowkey()].unlock();
    }
}

void process_rename_request(PennCloud::Request &request, PennCloud::Response &response)
{
    if(!request.has_rowkey() || !request.has_columnkey() || !request.has_value1() || !request.has_value2())
    {
        response.set_status(param_unset_message.first);
        response.set_description(param_unset_message.second);
    }
    else if(!is_rowkey_accepted(request.rowkey()))
    {
        response.set_status(invalid_rowkey_message.first);
        response.set_description(invalid_rowkey_message.second);
    }
    else
    {
        rowkey_lock[request.rowkey()].lock();
        if(kv_store.find(request.rowkey()) != kv_store.end())
        {
            if(kv_store[request.rowkey()].find(request.columnkey()) != kv_store[request.rowkey()].end())
            {
                string content = kv_store[request.rowkey()][request.columnkey()];
                const std::size_t pos = content.find("/" + request.value1() + ":");
                if (content[0] != '/') 
                {
                    response.set_status(inode_notdir_message.first);
                    response.set_description(inode_notdir_message.second);
                }
                else if (pos == std::string::npos) 
                {
                    response.set_status(inode_notexists_message.first);
                    response.set_description(inode_notexists_message.second);
                }
                else
                {
                    const std::size_t end = content.find("/", pos + 1);
                    const std::size_t prefix_len = pos + request.value1().size() + 2;
                    std::string inode = content.substr(prefix_len, end + 1 - prefix_len);
                    content.erase(pos + 1, end - pos);
                    if (request.value2().empty()) 
                    {
                        // kv_store[request.rowkey()][request.columnkey()] = content;
                        inode.pop_back();
                        if (inode.back() == 'd')
                        {
                            inode.pop_back();
                            const std::string content2 =  kv_store[request.rowkey()][inode];
                            if (content2.size() > 1)
                            {
                                response.set_status(directory_not_empty_message.first);
                                response.set_description(directory_not_empty_message.second);
                                rowkey_lock[request.rowkey()].unlock();
                                return;
                            }
                        } 
                        else 
                        {
                            inode.pop_back();
                        }
                        //dele(rkey, inode);
                        if(kv_store[request.rowkey()].find(inode) != kv_store[request.rowkey()].end())
                        {
                            kv_store[request.rowkey()].erase(inode);
                        }
                        //put(rkey, parent, content);
                        kv_store[request.rowkey()][request.columnkey()] = content;
                    }
                    else
                    {
                        const std::size_t pos2 = request.value2().find("%2F");
                        if (pos2 == std::string::npos) 
                        {
                            response.set_status(invalid_target_message.first);
                            response.set_description(invalid_target_message.second);
                        }
                        else
                        {
                            const std::string target = request.value2().substr(0, pos2);
                            const std::string ren = request.value2().substr(pos2 + std::strlen("%2F"));
                            std::string content2;
                            if (target == request.columnkey()) 
                            {
                                content2 = content;
                            } 
                            else 
                            {
                                if(kv_store[request.rowkey()].find(target) != kv_store[request.rowkey()].end())
                                {
                                    content2 = kv_store[request.rowkey()][target];
                                    if (content2[0] != '/') 
                                    {
                                        response.set_status(target_inode_notdir_message.first);
                                        response.set_description(target_inode_notdir_message.second);
                                    }

                                }
                                else
                                {
                                    response.set_status(target_inode_notexists_message.first);
                                    response.set_description(target_inode_notexists_message.second);
                                }
                            }
                            if (content2.find("/" + ren + ":") != std::string::npos) 
                            {
                                response.set_status(target_inode_exists_message.first);
                                response.set_description(target_inode_exists_message.second);
                            }
                            else
                            {
                                content2 += ren + ":" + inode;
                                if (target != request.columnkey()) 
                                {
                                    kv_store[request.rowkey()][request.columnkey()] = content;
                                }
                                kv_store[request.rowkey()][target] = content2;
                            }       
                        }                    
                    }
                    response.set_status("+OK");
                }
            }
            else
            {
                response.set_status(inode_inexistence_message.first);
                response.set_description(inode_inexistence_message.second);
            }
        }
        else
        {
            response.set_status(inode_inexistence_message.first);
            response.set_description(inode_inexistence_message.second);
        }
        rowkey_lock[request.rowkey()].unlock();
    }

}
