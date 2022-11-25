#include "globalvars.h"
void process_get_request(PennCloud::Request &request, PennCloud::Response &response);
void process_put_request(PennCloud::Request &request, PennCloud::Response &response);
void process_cput_request(PennCloud::Request &request, PennCloud::Response &response);
void process_delete_request(PennCloud::Request &request, PennCloud::Response &response);

bool is_rowkey_accepted(string rowkey)
{
    int start_letter = rowkey[0];
    for(int i = 0; i < rowkey_range.size(); i++)
    {
        if(rowkey[0] >= rowkey_range[i].first && rowkey[0] <= rowkey_range[i].second)
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
        kvstore_lock.lock();
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
        kvstore_lock.unlock();
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
        kvstore_lock.lock();
        if(kv_store.find(request.rowkey()) != kv_store.end())
        {
            kv_store[request.rowkey()][request.columnkey()] = request.value1();              
            response.set_status("+OK");
        }
        else
        {
            kv_store[request.rowkey()] = unordered_map<string, string>();
            kv_store[request.rowkey()][request.columnkey()] = request.value1();   
            response.set_status("+OK");
        }
        kvstore_lock.unlock();
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
        kvstore_lock.lock();
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
        kvstore_lock.unlock();
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
        kvstore_lock.lock();
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
        kvstore_lock.unlock();
    }
}