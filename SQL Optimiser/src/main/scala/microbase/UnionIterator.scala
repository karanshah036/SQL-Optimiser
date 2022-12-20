package microbase

import org.apache.spark.sql.catalyst.InternalRow

case class UnionIterator(firstIterator:Iterator[InternalRow],secondIterator:Iterator[InternalRow]) extends Iterator[InternalRow]
{
    def hasNext: Boolean = firstIterator.hasNext || secondIterator.hasNext
    def next(): InternalRow =
    {
        if(firstIterator.hasNext)
        {
            val tuple = firstIterator.next()
            tuple
        }
        else
        {
            val tuple = secondIterator.next()
            tuple
        }
    }
}