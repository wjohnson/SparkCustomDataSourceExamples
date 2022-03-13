package com.dataguidebook.spark.datasource.v1;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// For moving from List to Seq
import scala.collection.JavaConversions.*;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
// For defining the raw data
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
// For defining the relation and its abilities
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.TableScan;

// For working with the schema
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.StructField;

// For creating data
import org.apache.spark.sql.RowFactory;

public class CustomDataRelation extends BaseRelation implements Serializable, TableScan {
    private SQLContext sqlContext;
    private StructType cachedSchema;
    private String path;

    @Override
    public SQLContext sqlContext() {
        return this.sqlContext;
    }

    public void setSqlContext(SQLContext sqlContext) {
        this.sqlContext = sqlContext;
    }

    public CustomDataRelation(SQLContext sqlContext, String path, StructType userSchema) {
        this.sqlContext = sqlContext;
        this.path = path;
        this.cachedSchema = userSchema;

    }

    /**
     * Build and returns the schema as a StructType.
     */
    @Override
    public StructType schema() {

        if (cachedSchema == null) {

            // Setting up a dummy StructField
            StructField[] fields = new StructField[3];
            fields[0] = DataTypes.createStructField("column01", DataTypes.IntegerType, true);
            fields[1] = DataTypes.createStructField("column02", DataTypes.IntegerType, true);
            fields[2] = DataTypes.createStructField("column03", DataTypes.IntegerType, true);
            cachedSchema = new StructType(fields);
        }
        return cachedSchema;
    }

    @Override
    public RDD<Row> buildScan() {

        // Creating some dummy data
        List<Row> dataToParallelize = new ArrayList<Row>();
        dataToParallelize.add(RowFactory.create(1, 2, 3));
        dataToParallelize.add(RowFactory.create(4, 5, 6 ));
        dataToParallelize.add(RowFactory.create(7, 8, 9 ));

        JavaSparkContext sparkContext = new JavaSparkContext(sqlContext.sparkContext());
        JavaRDD<Row> rowRDD = sparkContext.parallelize(dataToParallelize);
        return rowRDD.rdd();
    }
}
