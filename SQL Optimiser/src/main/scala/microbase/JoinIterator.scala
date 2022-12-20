package microbase

import org.apache.spark.sql.catalyst.InternalRow
import java.beans.Expression
import org.apache.spark.sql.catalyst.expressions

case class JoinIterator(boundCondition: expressions.Expression,leftChildIterator:Iterator[InternalRow],righrChildIterator:Iterator[InternalRow]) extends Iterator[InternalRow] 
{
    def hasNext: Boolean = ???
    def next(): InternalRow = ???
}