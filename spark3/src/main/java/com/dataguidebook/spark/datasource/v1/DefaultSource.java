package com.dataguidebook.spark.datasource.v1;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.RelationProvider;

import scala.Option;
import scala.collection.immutable.Map;

public class DefaultSource implements RelationProvider {

  // Overrides RelationProvider's signature for createRelation
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
      
}
