
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


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}    

ip2region_entry ip2rEntry;


void initIps(){

    std::string dbFile="/home/huamin/src/ClickHouse/src/Functions/greenet/conf/ip2region.db";

    datablock_entry datablock;
    memset(&datablock, 0x00, sizeof(datablock_entry));

    if (ip2region_create(&ip2rEntry, dbFile.c_str()) == 0) {
        println("Error: Fail to create the ip2region object\n");
    };

    std::cout<<"init gnip "<<std::endl;
}

class FunctionGnIp : public IFunction
{
public:

    static constexpr auto name = "gnip";
    static FunctionPtr create(const Context & con)
    {
        
        std::cout<<"gnip: create "<<con.getPath()<<std::endl;

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
    
            datablock_entry datablock;

            ip2region_memory_search_string(&ip2rEntry, col->getChars().raw_data(), &datablock);

            auto col_res = ColumnString::create();
    
            std::string res=datablock.region;

            std::cout<<"result:"<<result<<",size:"<<res.size()<<",length:"<<res.length()<<std::endl;
            std::cout<<"gnip input:"<<col->getChars().raw_data()<<",outpu:"<<res.c_str()<<std::endl;

            //col_res->insertData(res.c_str(),res.length());
            //block.getByPosition(result).column = std::move(col_res);
            block.getByPosition(result).column = DataTypeString().createColumnConst(input_rows_count, res);
        }
        else
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

void registerFunctionGnIp(FunctionFactory & factory)
{
    initIps();
    factory.registerFunction<FunctionGnIp>();

}

}


