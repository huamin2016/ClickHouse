
#include "libip.h"
/*
#include "ip2region.h"
#include <string>
*/

/*

*/
int init_iplib(ip2region_entry *ip2rEntry, std::string dbfile, datablock_entry* datablock) {
	memset(datablock, 0x00, sizeof(datablock_entry));
	ip2region_create(ip2rEntry, dbfile.c_str());
	return 0;
}

/*

*/
void get_ip(ip2region_entry *ip2rEntry, std::string rawip, datablock_entry *datablock) {

	ip2region_memory_search_string(ip2rEntry, rawip.c_str(), datablock);

}
