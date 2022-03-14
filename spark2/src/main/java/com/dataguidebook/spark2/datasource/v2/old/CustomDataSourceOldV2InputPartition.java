package com.dataguidebook.spark2.datasource.v2.old;

import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.sql.catalyst.InternalRow;
// Internal Row expects a scala.collection.Seq
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.List;
import java.util.Arrays;

public class CustomDataSourceOldV2InputPartition implements InputPartition<InternalRow>, InputPartitionReader<InternalRow> {
    private List<Integer> fakeData = Arrays.asList(1,2,3);
    private int index = 0;

    @Override
    public InputPartitionReader<InternalRow> createPartitionReader(){
        return new CustomDataSourceOldV2InputPartition();
    }

    @Override
    public InternalRow get(){
        List<Object> internalRowList = Arrays.asList(fakeData.get(index).intValue(), 10, 20);

        Seq s = JavaConverters.asScalaIteratorConverter(internalRowList.iterator()).asScala().toSeq();
        InternalRow response = InternalRow.apply(s);
        index = index + 1;
        return response;
    }

    @Override
    public boolean next(){
        return index < fakeData.size();
    }

    @Override
    public void close(){

    }
}
