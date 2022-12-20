package microbase

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.expressions.Expression

case class ProjectIterator(boundedRef: Seq[Expression], childIterator : Iterator[InternalRow]) extends Iterator[InternalRow]
{
    def hasNext: Boolean = childIterator.hasNext
    def next(): InternalRow =
    {
        var tuple = childIterator.next()
        if(tuple==null)
        {
            null
        }
        else
        {
            val newRow: Seq[Any]=boundedRef.map(expr=>expr.eval(tuple))
            InternalRow.fromSeq(newRow)
        }
    }
}