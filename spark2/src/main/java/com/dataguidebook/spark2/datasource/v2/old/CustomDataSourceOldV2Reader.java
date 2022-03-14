package com.dataguidebook.spark2.datasource.v2.old;

import org.apache.spark.sql.sources.v2.reader.DataSourceReader;


// For working with the schema
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.StructField;

// For working with the InputPartition
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.sql.catalyst.InternalRow;


public class CustomDataSourceOldV2Reader implements DataSourceReader{

    @Override
    public StructType readSchema(){
        StructField[] fields = new StructField[3];
        fields[0] = DataTypes.createStructField("column01", DataTypes.IntegerType, true);
        fields[1] = DataTypes.createStructField("column02", DataTypes.IntegerType, true);
        fields[2] = DataTypes.createStructField("column03", DataTypes.IntegerType, true);
        return new StructType(fields);
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions(){
        List<InputPartition<InternalRow>> inputPartitions = new ArrayList<>();
        inputPartitions.add(new CustomDataSourceOldV2InputPartition());
        return inputPartitions;
    }
}
