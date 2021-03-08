
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionDateOrDateTimeAddInterval.h>

#include <common/DateLUTImpl.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>

#include <Columns/ColumnsNumber.h>

#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/castTypeToEither.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>

#include <IO/WriteHelpers.h>

#include <iostream>
#include <string>
#include <Columns/ColumnString.h>
#include "Functions/greenet/ip/libip.h"
#include <DataTypes/DataTypeString.h>

#include <Poco/Path.h>
#include <stdlib.h>
#include <Poco/Logger.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}    

char* DIC_GNIP=nullptr;
ip2region_entry ip2rEntry;
//Poco::Logger log;

void initIps(){

    datablock_entry datablock;
    memset(&datablock, 0x00, sizeof(datablock_entry));

    if (ip2region_create(&ip2rEntry, DIC_GNIP) == 0) {
        println("Error: Fail to create the ip2region object\n");
    };
    std::cout<<"init dic_gnip "<<std::endl;
}

class FunctionGnIp : public IFunction
{
public:

    static constexpr auto name = "gnip";
    static FunctionPtr create(const Context & con)
    {
        //std::cout<<"gnip: create "<<con.getPath()<<std::endl;
        return std::make_shared<FunctionGnIp>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool isInjective(const Block &) const override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {

            auto col_res = ColumnString::create();
            vector(col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets());
            block.getByPosition(result).column = std::move(col_res);
    
            //datablock_entry datablock;
            //ip2region_memory_search_string(&ip2rEntry, col->getChars().raw_data(), &datablock);
            //std::string res=datablock.region;
            //auto col_res = ColumnString::create();
            //std::cout<<"gnip input:"<<col->getChars().raw_data()<<",outpu:"<<res.c_str()<<std::endl;
            //block.getByPosition(result).column = DataTypeString().createColumnConst(input_rows_count, res);


        }
        else
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

using Pos=const char *;

static void vector(const ColumnString::Chars & data, const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data, ColumnString::Offsets & res_offsets)
    {
        size_t size = offsets.size();
        res_offsets.resize(size);
        res_data.reserve(size * 32);

        size_t prev_offset = 0;
        size_t res_offset = 0;

        /// Matched part.
        Pos start;
        size_t length;

        for (size_t i = 0; i < size; ++i)
        {
            execute(reinterpret_cast<const char *>(&data[prev_offset]), offsets[i] - prev_offset - 1, start, length);

            res_data.resize(res_data.size() + length + 1);
            memcpySmallAllowReadWriteOverflow15(&res_data[res_offset], start, length);
            res_offset += length + 1;
            res_data[res_offset - 1] = 0;

            res_offsets[i] = res_offset;
            prev_offset = offsets[i];
        }
    }

  static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
    {
        res_data = "";
        res_size = 0;
        
        datablock_entry datablock;
        ip2region_memory_search_string(&ip2rEntry, data, &datablock);
        
        res_data=datablock.region;
        res_size=strlen(res_data); 

        std::cout<<"in:"<<data<<",out:"<<res_data<<std::endl;
    }


};

void registerFunctionGnIp(FunctionFactory & factory)
{
    DIC_GNIP=getenv("DIC_GNIP");
    if(DIC_GNIP!=nullptr){ 
        initIps();
        factory.registerFunction<FunctionGnIp>();
    }
}

}


