package org.mimirdb.iskra

import org.apache.spark.sql.catalyst.expressions._

object JoinPredicates
{
  /**
   * Get the attributes of an expression
   * 
   * @param  expression  An expression tree
   * @return             All Attribute nodes in the expression tree
   */
  def getAttributes(expr: Expression): Set[Attribute] = 
    expr match { 
      case a: Attribute => Set(a)
      case _ => expr.children.map { getAttributes(_) }.fold(Set.empty) { _ & _ }
    }


  /**
   * Extract an equijoin out of an arbitrary condition
   * 
   * @param   condition  The condition to extract equijoin fields out of
   * @param   lhs        The LHS schema
   * @param   rhs        The RHS schema
   * @return             A triple consisting of the LHS join terms, RHS join terms, 
   *                     and a "remainder" of any expression terms that can't be
   *                     expressed in the equijoin
   */
  def getEquiJoinAttributes(
    condition: Expression, 
    lhsSchema: Set[Attribute], 
    rhsSchema: Set[Attribute]
  ): (Seq[Expression], Seq[Expression], Option[Expression]) =
  {
    condition match {
      case EqualTo(eqLhs, eqRhs) => 
        val eqLhsAttrs = getAttributes(eqLhs)
        val eqRhsAttrs = getAttributes(eqRhs)
        // if the comparison is against a constant, this isn't a valid join condition
        if(eqLhsAttrs.isEmpty || eqRhsAttrs.isEmpty){
          return (Seq.empty, Seq.empty, Some(condition))
        }
        // LHS.attrs = RHS.attrs
        if(eqLhsAttrs.subsetOf(lhsSchema) && eqRhsAttrs.subsetOf(rhsSchema)){
          return (Seq(eqLhs), Seq(eqRhs), None)
        }
        // RHS.attrs = LHS.attrs
        if(eqLhsAttrs.subsetOf(rhsSchema) && eqRhsAttrs.subsetOf(lhsSchema)){
          return (Seq(eqRhs), Seq(eqLhs), None)
        }
        // mixed attributes on both sides
        return (Seq.empty, Seq.empty, Some(condition))
      case And(andLhs, andRhs) => 
        val (andLhsExtractedLhs, andLhsExtractedRhs, andLhsRemainder) = 
          getEquiJoinAttributes(andLhs, lhsSchema, rhsSchema)
        val (andRhsExtractedLhs, andRhsExtractedRhs, andRhsRemainder) = 
          getEquiJoinAttributes(andRhs, lhsSchema, rhsSchema)
        (
          andLhsExtractedLhs ++ andRhsExtractedLhs,
          andLhsExtractedRhs ++ andRhsExtractedRhs,
          (andLhsRemainder, andRhsRemainder) match {
            case (None, remainder) => remainder
            case (remainder, None) => remainder
            case (Some(l), Some(r)) => Some(And(l, r))
          }
        )
    }
  }

}