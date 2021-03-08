#pragma once

#include "ip2region.h"
#include <string>

/*

*/
int init_iplib(ip2region_entry* ip2rEntry, std::string dbfile, datablock_entry* datablock);

/*

*/
void get_ip(ip2region_entry* ip2rEntry, std::string rawip, datablock_entry* datablock);
