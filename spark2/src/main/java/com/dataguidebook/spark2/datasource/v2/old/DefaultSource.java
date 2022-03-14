package com.dataguidebook.spark2.datasource.v2.old;

import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;

import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;


public class DefaultSource implements DataSourceV2, ReadSupport{

    @Override
    public DataSourceReader createReader(DataSourceOptions options){
        return new CustomDataSourceOldV2Reader();
    }
    
}
