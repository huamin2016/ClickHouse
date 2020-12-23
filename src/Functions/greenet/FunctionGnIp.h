#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>
#include <iostream>
#include <Columns/ColumnString.h>
#include <string>
#include <Functions/greenet/ip/libip.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


class FunctionGnIp : public IFunction
{
public:

    static constexpr auto name = "gnip";
    static FunctionPtr create(const Context &)
    {
        std::cout<<"gnip: create"<<std::endl;

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

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) const override
    {
        const ColumnPtr column = block.getByPosition(arguments[0]).column;
        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
    
            datablock_entry datablock;

            ip2region_memory_search_string(&ip2rEntry, col->getChars().raw_data(), &datablock);

            auto col_res = ColumnString::create();
    
            std::string res=datablock.region;

            col_res->insertData(res.c_str());

            block.getByPosition(result).column = std::move(col_res);
            //block.getByPosition(result).column = DataTypeString().createColumnConst(input_rows_count, db_name);
        }
        else if (const ColumnFixedString * col_fixed = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            auto col_res = ColumnFixedString::create(col_fixed->getN());

            std::cout<<"ColumnFixedString column:"<<column<<" "<<col->getChars().raw_data()<<std::endl;

            //Impl::vectorFixed(col_fixed->getChars(), col_fixed->getN(), col_res->getChars());
            block.getByPosition(result).column = std::move(col_res);
        }
        else
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

}
