#include "stdc++.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fstream>
using namespace std;

/* Create a directory if it doesn't already exist */
void create_dir(string dir){
    int status;
    status = mkdir(dir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
}

/* Clear content of the guven file*/
void clear_file(string file_name){
    ofstream ofs;
    ofs.open(file_name, std::ofstream::out | std::ofstream::trunc);
    ofs.close();
}

void create_file(string dest){
    dest = "../" + dest;
    cout<<dest<<endl;
    ofstream new_file;
    new_file.open(dest, fstream::app);
    new_file.close();
}


