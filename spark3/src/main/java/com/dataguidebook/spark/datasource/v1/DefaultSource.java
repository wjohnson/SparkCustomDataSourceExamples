package com.dataguidebook.spark.datasource.v1;

import org.apache.spark.sql.SQLContext;

// Core class to inherit from
import org.apache.spark.sql.sources.BaseRelation;
// For reading without a schema
import org.apache.spark.sql.sources.RelationProvider;

// For writing a dataframe
import org.apache.spark.sql.sources.CreatableRelationProvider;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

// For handling the required Map and extracting values from the optionals
// that are used when passing parameters to the dataframe reader or writer
import scala.Option;
import scala.collection.immutable.Map;

public class DefaultSource implements RelationProvider, CreatableRelationProvider {

  // From Relationprovider: Defines a way to get a relation when no schema is
  // provided. There's a SchemaRelationProvider that you can use when a
  //schema is provided
    @Override
  public BaseRelation createRelation(
      SQLContext sqlContext,
      Map<String, String> params) {
        Option<String> parameterPath = params.get("path");
        if (parameterPath.isEmpty()){
          throw new IllegalArgumentException("The path can't be empty");
        }

        CustomDataRelation relation = new CustomDataRelation(sqlContext, parameterPath.get(), null );
        return relation;
      }

    // From CreateableRealtionProvider: Define the business logic for writing
    // a dataframe to your custom data source for a given SaveMode
    @Override
    public BaseRelation createRelation(SQLContext sqlContext, SaveMode mode, Map<String, String> parameters,
        Dataset<Row> data) {
      System.out.println(String.format("Mode: %s", mode.toString()));
      Option<String> parameterPath = parameters.get("path");
      if (parameterPath.isEmpty()){
        throw new IllegalArgumentException("The path can't be empty");
      }
      
      // Define business logic for each operation if required
      if (mode == SaveMode.Append){
        System.out.println("Appending");
      }else if (mode == SaveMode.ErrorIfExists){
        System.out.println("Should error if exists");
      }else if (mode == SaveMode.Ignore){
        System.out.println("Should ignore");
      }else if (mode == SaveMode.Overwrite){
        System.out.println("Overwriting");
      }

      // You can then call the non-SaveMode constructor to get back a relation
      return createRelation(sqlContext, parameters);
    }
      
}
