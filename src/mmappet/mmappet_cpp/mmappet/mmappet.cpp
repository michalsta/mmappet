#include "mmappet.h"


int main()
{
auto D = OpenDataset<uint32_t,uint32_t,uint32_t, uint32_t, uint32_t>("/Users/mist/git/massimo_cpp/test1.mmappet");

std::cout << D.get_column<4>()[6] << "\n";
    return 0;
}