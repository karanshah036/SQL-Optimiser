package microbase
import java.io.File
import scala.io._
import scala.io.StdIn.readLine
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.CreateTableStatement
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst.expressions.AttributeSeq
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.commons.lang3.ObjectUtils
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import com.fasterxml.jackson.module.scala.deser.overrides

case class MyTable(tableName: Seq[String], tableSchema: StructType,location: Option[String]) extends LeafNode
{  
    
    var attrRef =  Seq[Attribute]()//scala.collection.mutable.ArrayBuffer.empty[Attribute]
    
    for (x <- tableSchema)
    {
        attrRef = attrRef :+ (new AttributeReference(x.name,x.dataType)(qualifier=tableName))
    }
    override def  output: Seq[Attribute] =
    {
       var seqOfAttrRef:Seq[org.apache.spark.sql.catalyst.expressions.Attribute]=attrRef
        seqOfAttrRef
    }
}
    

// } //return attribute of the table
// AttributeSeq is a subclass of Seq[Attribute]