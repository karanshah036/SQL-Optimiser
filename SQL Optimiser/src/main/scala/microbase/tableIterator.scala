package microbase
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import scala.io.Source
import java.sql.Date
import org.apache.spark.unsafe.types.UTF8String
import java.time.LocalDate

case class TableIterator(tableSchema : StructType,location:Option[String]) extends Iterator[InternalRow]
{
    var fileIterator = Source.fromFile(location.get).getLines 
    def hasNext: Boolean = fileIterator.hasNext
    def next(): InternalRow = 
    {
        // var singleRow=fileIterator.next()
        var singleRowString=fileIterator.next().split('|')
        var newSeq:Seq[Any]=Seq[Any]()
        var count=0
        for(y <- tableSchema)
        {
            var x=singleRowString(count)
                y.dataType match{
                    case IntegerType => newSeq=newSeq:+(x.toInt)
                    case StringType => newSeq=newSeq:+(UTF8String.fromString(x))
                    case FloatType => newSeq=newSeq:+(x.toFloat)
                    case DoubleType => newSeq=newSeq:+(x.toDouble)
                    case DateType => newSeq=newSeq:+(Date.valueOf(x))
                }
                count+=1
        }   
        InternalRow.fromSeq(newSeq)
    }
}

    // case class TableIterator(tableSchema: StructType, fileIterator : Iterator[String]) extends Iterator[InternalRow]
    // {

    //     def hasNext: Boolean = fileIterator.hasNext
    //     def next(): InternalRow =
    //     {
    //         var singleRow=fileIterator.next()
    //         var singleRowString=singleRow.split('|')
    //         var newSeq:Seq[Any]=Seq[Any]()
    //         var count=0
    //         for(y <- tableSchema)
    //         {
    //             var x=singleRowString(count)
    //                 y.dataType match{
    //                     case IntegerType => newSeq=newSeq:+(x.toInt)
    //                     case StringType => newSeq=newSeq:+(x)
    //                     case FloatType => newSeq=newSeq:+(x.toFloat)
    //                     case DoubleType => newSeq=newSeq:+(x.toDouble)
    //                 }
    //                 count+=1
    //         }   
    //         InternalRow.fromSeq(newSeq)
    //     }   
    // }