import pyspark.sql.functions as F

###Function to filter rows in a pyspark DataFrame which contain a subtring that  ###
###does (or does not) match any item in a  list.                                 ###

#Usage:
#df (pyspark.sql.dataframe.DataFrame): source DataFrame
#col (String): column name to evaluate
#list1 (list): list of items to compare in DataFrame
#isLike (boolean, Default = True): flag to determine if comparison filter 
#      should be "is like" (True) or "is not like" (False)
#returns resulting pyspark.sql.dataframe.DataFrame

def likeInList(df, col, list1, isLike = True):
  #Get schema from source DataFrame
  schema = df.schema
  #Create empty DataFrame using schema
  dfNew = spark.createDataFrame([],schema)
  
  #Generate a new dataframe containing only rows where column value
  #   contains a substring matching any of the items in the list
  for x in list1:
     dfNew = dfNew.union(df.where(F.col(col).like(f"%{x}%")))
      
  if not isLike:
     # return DataFrame with items not in the list
     return df.subtract(dfNew)
  else:
     return dfNew
  return dfNew
