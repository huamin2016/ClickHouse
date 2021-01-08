#include "gnlib.h"
#include <iostream>

using namespace std;

void gnprint(const string &info){
    cout<<"address:"<<&info<<",value-->"<<info<<endl;
}
