package microbase 
import java.io.File
import util.control.Breaks._
import scala.io._
import scala.io.StdIn.readLine
import org.apache.spark.unsafe.types.UTF8String
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
import MyTable._
import TableIterator._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.plans.logical.Project
import scala.collection.mutable.ArrayStack
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.expressions.EqualTo
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.NaturalJoin
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.logical.Union
import org.apache.spark.sql.catalyst.analysis.UnresolvedAlias
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.plans.logical.Subquery
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.catalyst.plans.logical.Sort
import _root_.org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical.Limit
import org.apache.spark.sql.catalyst.plans.logical.GlobalLimit
import org.apache.spark.sql.catalyst.plans.logical.LocalLimit
import org.apache.spark.sql.catalyst.expressions.Literal

object Microbase
{
    //CREATE TABLE R(A int, B int, C int) USING csv OPTIONS(path 'data/R.data', delimiter'|');
    //CREATE TABLE S(A int, L int, M int) USING csv OPTIONS(path 'data/S.data', delimiter'|');
    //CREATE TABLE T(L int, X int, Y int) USING csv OPTIONS(path 'data/S.data', delimiter'|');

    def main(args:Array[String]):Unit=
    { 
        var tableRef = scala.collection.mutable.HashMap[Seq[String],MyTable]()
        var input:String=""
        var logical_plan:LogicalPlan=null;
        var resolvedRelation:LogicalPlan=null;
        def parseSql(sql: String): LogicalPlan =  new SparkSqlParser().parsePlan(sql)
        def bindReferences(expression:Expression , child:LogicalPlan) : Expression =
        {
            bindReference(expression,child.output)
        }
        def bindReference(expression:Expression , input : Seq[Attribute]) : Expression =
        {
            val inputPosition = input.map(_.exprId).zipWithIndex.toMap
            expression.transformDown
            {
                case attrRef: AttributeReference =>
                {
                    BoundReference(inputPosition(attrRef.exprId),attrRef.dataType,attrRef.nullable)
                }
            }
        }
        def compare(a:InternalRow,b:InternalRow,order:Seq[SortOrder]) : Boolean=
        {
            false
        }
        def startEval(logical_plan : LogicalPlan) : Iterator[InternalRow] =
        {
            logical_plan match 
            {
                case Project(projectList, child)=> 
                {
                    var childIterator : Iterator[InternalRow] = startEval(child)
                    val compiledExprs = projectList.map(bindReferences(_,child))
                    ProjectIterator(compiledExprs,childIterator)
                }
                case Filter(condition, child) =>
                 {
                     var childIterator : Iterator[InternalRow] = startEval(child)
                     var boundCondition : Expression = bindReferences(condition,child)
                     FilterIterator(boundCondition,childIterator)
                 }
                case MyTable(tableName, tableSchema, location) => 
                {
                    TableIterator(tableSchema,location)
                }
                case Union(children:Seq[LogicalPlan],_,_)=>
                {
                    val childIterator=children.map(startEval(_))
                    val firstIterator=childIterator.head
                    val secondIterator=childIterator.tail.head
                    UnionIterator(firstIterator,secondIterator)
                }
                case SubqueryAlias(identifier,child)=>
                {
                    startEval(child)
                }
                case subplan:Subquery => startEval(subplan.children(0))
                case Sort(order,global,child)=>
                {
                    var childIterator=startEval(child).toSeq
                    println("Order: "+order)
                    var x=childIterator.sortWith((a,b)=>compare(a,b,order))
                    println("x="+x)
                    childIterator.iterator
                }
                case GlobalLimit(limitExpr,child)=>
                {
                    var childIterator=startEval(child).slice(0,limitExpr.toString().toInt) 
                    childIterator
                }
                case LocalLimit(limitExpr,child)=>
                {
                    var childIterator=startEval(child).slice(0,limitExpr.toString().toInt) 
                    childIterator
                }
                case Join(left,right,joinType,condition,hint) => 
                {
                    var leftChildIterator : Iterator[InternalRow] = startEval(left)
                    var rightChildIterator = startEval(right).toSeq
                    leftChildIterator.flatMap 
                    { 
                        leftRow => 
                        rightChildIterator.map 
                        { 
                            rightRow => 
                            new JoinedRow(leftRow, rightRow)
                        }
                    }
                }

            }
        } 
        def resolveProject(projectList:Seq[NamedExpression],child:LogicalPlan):Seq[NamedExpression]=
        { 
            var newList:Seq[NamedExpression] = Seq[NamedExpression]()
            for(x <- projectList)
            {
                x match {
                    case UnresolvedStar(target) => 
                    {
                        if(target==None)
                        {
                            newList = newList ++ child.output
                        }
                        else
                        {
                            for(y <- child.output)
                            {
                                if (target.get(0).equals(y.qualifier.head))
                                {
                                    newList = newList :+ y
                                }
                            }
                        }
                    }
                    case UnresolvedAttribute(nameParts) => 
                    {
                        for(y <- child.output)
                        {
                            if(x.name.equals(y.name) || x.name.equals(y.qualifiedName))
                            {
                                newList = newList :+ y
                            }
                        }
                    }
                    case UnresolvedAlias(childExp,aliasFunc)=>
                    {
                        var ChildExp=childExp.transformUp
                        {
                            case UnresolvedAttribute(nameParts) => 
                            {
                                var newAttribute = scala.collection.mutable.ArrayBuffer.empty[Attribute]
                                for(z <- child.output)
                                {
                                    if (nameParts.last.equals(z.name))
                                    {
                                        newAttribute.append(z)
                                    }
                                }
                                newAttribute.head
                            }
                        }
                        var name=""
                        var newALias= Alias(ChildExp,name)()
                        newList=  newList :+ newALias
                    }
                    case Alias(childExp, name) => 
                    {
                       var ChildExp=childExp.transformUp
                        {
                            case UnresolvedAttribute(nameParts) => 
                            {
                                 var newAttribute = scala.collection.mutable.ArrayBuffer.empty[Attribute]
                                    for(z <- child.output)
                                    {
                                        if (nameParts.last.equals(z.name))//|| nameParts(0).equals(z.qualifier.head))
                                        {
                                            if(nameParts.length>1 && nameParts(0).equals(z.qualifier.head))
                                            {
                                                newAttribute.append(z)
                                            }
                                            else
                                            {
                                                newAttribute.append(z)
                                            }
                                            
                                        }
                                    }
                                    newAttribute.head
                            }
                        }
                        var newALias= Alias(ChildExp,name)()
                        newList=  newList :+ newALias
                    }
                }
            }
            newList
        }
        while(true)
        {
            println("$>")
            input=readLine()
            logical_plan=parseSql(input)
            print(logical_plan.treeString)
            
            logical_plan match
            {
                case c: CreateTableStatement =>
                {
                    /* do something with c.name, c.tableSchema */
                    var tableobj= new MyTable(c.tableName,c.tableSchema,c.location)
                    tableRef += (c.tableName -> tableobj)
                }
                
                case _ =>/* Interpret plan like a query */
                {
                    logical_plan = logical_plan.transform
                    {
                        case UnresolvedRelation(nameElements, _, _) =>
                        {  
                            tableRef(nameElements)
                        }
                    }
                    
                    logical_plan = logical_plan.transformUp
                    {    
                        case Project(projectList,child)=>
                        {   var temp:LogicalPlan=null
                            var newList = resolveProject(projectList,child)
                            temp=Project(newList,child)
                            temp
                        }
                        case Filter(condition, child) => 
                        {
                            var Condition=condition.transformUp
                            {
                                case UnresolvedAttribute(nameParts)=>
                                {
                                    var newAttribute = scala.collection.mutable.ArrayBuffer.empty[Attribute]
                                    for(z <- child.output)
                                    {
                                        if(nameParts.length>1)
                                        {
                                            if(nameParts(0).equals(z.qualifier.head))
                                            {
                                                newAttribute.append(z)
                                            }
                                        }
                                        else if (nameParts.last.equals(z.name))
                                        {
                                            newAttribute.append(z)
                                        }                                     
                                    }
                                    newAttribute.head
                                }
                            }
                            Filter(Condition,child)
                        }       
                        case Join(left,right,joinType,condition,hint)=>
                        {
                        //    if (joinType.isInstanceOf(NaturalJoin))
                        //    {
                        //        Join(left,right,joinType=Inner,condition,hint)
                        //    }
                            if(condition.equals(None))
                            {
                                Join(left,right,joinType,condition,hint)
                            }
                            //Join(left,right,joinType,condition,hint)
                            else
                            {
                                var Condition = condition.get.transformUp
                                {
                                    case UnresolvedAttribute(nameParts) => 
                                    {
                                        var newAttribute = scala.collection.mutable.ArrayBuffer.empty[Attribute]
                                        if(nameParts.head.equals(left.output(0).qualifier.head))
                                        {
                                            for(x <- left.output)
                                            {
                                                if (nameParts(0).equals(x.name) || nameParts(0).equals(x.qualifier.head))
                                                {
                                                    newAttribute.append(x)
                                                }
                                            } 
                                        }
                                        if(nameParts.head.equals(right.output(0).qualifier.head))
                                        {
                                            for(x <- right.output)
                                            {
                                                if (nameParts(0).equals(x.name) || nameParts(0).equals(x.qualifier.head))
                                                {
                                                    newAttribute.append(x)
                                                }
                                            }                  
                                        }
                                        newAttribute.head
                                    }
                                }
                                Join(left,right,joinType,new Some(Condition),hint)
                            }
                        }  
                        case Sort(order,global,child)=>
                        {
                            var newOrder:Seq[SortOrder]=Seq[SortOrder]()
                            for(eachOrder <- order)
                            {
                                //println("sortOrder expression: "+eachOrder.child)
                                var resolvedChild=eachOrder.child.transformUp
                                {
                                    case UnresolvedAttribute(nameParts) => 
                                    {
                                        var newAttribute = scala.collection.mutable.ArrayBuffer.empty[Attribute]
                                        for(z <- child.output)
                                        {
                                            if(nameParts.length>1)
                                            {
                                                if(nameParts(0).equals(z.qualifier.head))
                                                {
                                                    newAttribute.append(z)
                                                }
                                            }
                                            else if (nameParts.last.equals(z.name))
                                            {
                                                newAttribute.append(z)
                                            }                                     
                                        }
                                        newAttribute.head
                                    }
                                }
                                newOrder=newOrder :+ SortOrder(resolvedChild,eachOrder.direction,eachOrder.nullOrdering,eachOrder.sameOrderExpressions)
                            }
                            Sort(newOrder,global,child)
                        }
                    }
                }
             }
             if (logical_plan.resolved)
             {
                 //optimise(logical_plan)
                 var iterator=startEval(logical_plan)
                 while(iterator.hasNext)
                 {
                    val value=iterator.next()
                    //println(value)
                    breakable
                    {
                        if(value==null)
                        {
                            break
                        }
                        else
                        {
                            for(i<-0 to logical_plan.output.length-2)
                            {
                                print(value.get(i,logical_plan.output(i).dataType))
                                print("|")
                            }
                            print(value.get(logical_plan.output.length-1,logical_plan.output(logical_plan.output.length-1).dataType))
                            //println(value)
                            println()
                        }
                    }     
                 }
             }
             println(logical_plan.treeString)
             println(logical_plan.resolved)
               
         }
    }
}