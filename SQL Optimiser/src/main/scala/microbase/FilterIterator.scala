package microbase

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression

case class FilterIterator(boundRef:Expression,childIterator:Iterator[InternalRow]) extends Iterator[InternalRow]
{
    def hasNext: Boolean = childIterator.hasNext
    def next(): InternalRow = 
    {
        val tuple = childIterator.next()
        var x = boundRef.eval(tuple).asInstanceOf[Boolean]
        if(x==true)
        {
            tuple
        }
        else
        {
            if(this.hasNext)
            {
                this.next()
            }
            else
                return null
        }
    }
}