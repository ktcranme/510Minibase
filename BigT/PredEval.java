package BigT;

import heap.*;
import iterator.CondExpr;
import iterator.PredEvalException;
import iterator.UnknowAttrType;
import global.*;
import java.io.*;

public class PredEval {
    public static<T> int compare(T a, T b){
        if (a instanceof Comparable) 
            if (a.getClass().equals(b.getClass()))
                return ((Comparable<T>)a).compareTo(b);     
        throw new UnsupportedOperationException();
    }

    public static boolean Eval(CondExpr conditions[], Map map)
    throws IOException,
    UnknowAttrType,
    InvalidTypeException,
    FieldNumberOutOfBoundException,
    PredEvalException {
		CondExpr temp_ptr;

        AttrType comparison_type = new AttrType(AttrType.attrInteger);
        int comp_res;
        boolean op_res = false, row_res = false, col_res = true;

        if (conditions == null || conditions.length == 0)
            return true;

        for (CondExpr condition: conditions) {
			temp_ptr = condition;
			if (condition == null) continue;
            while (temp_ptr != null) {
				Object a = null, b = null;

				switch (temp_ptr.type1.attrType) {
                    case AttrType.attrInteger:
                        a = temp_ptr.operand1.integer;
                        comparison_type.attrType = AttrType.attrInteger;
                        break;
                    case AttrType.attrReal:
						a = temp_ptr.operand1.real;
                        comparison_type.attrType = AttrType.attrReal;
                        break;
                    case AttrType.attrString:
						a = temp_ptr.operand1.string;
                        comparison_type.attrType = AttrType.attrString;
                        break;
					case AttrType.attrSymbol:
						switch (temp_ptr.operand1.symbol.offset) {
							case 1:
								a = map.getRowLabel();
								comparison_type.attrType = AttrType.attrString;
								break;
							case 2:
								a = map.getColumnLabel();
								comparison_type.attrType = AttrType.attrString;
								break;
							case 3:
								a = map.getTimeStamp();
								comparison_type.attrType = AttrType.attrInteger;
								break;
							case 4:
								a = map.getValue();
								comparison_type.attrType = AttrType.attrString;
								break;
							default:
								throw new FieldNumberOutOfBoundException();
						}
                        break;
					default:
						throw new InvalidTypeException();
                }

				switch (temp_ptr.type2.attrType) {
                    case AttrType.attrInteger:
                        b = temp_ptr.operand2.integer;
                        break;
                    case AttrType.attrReal:
						b = temp_ptr.operand2.real;
                        break;
                    case AttrType.attrString:
						b = temp_ptr.operand2.string;
                        break;
					case AttrType.attrSymbol:
						switch (temp_ptr.operand2.symbol.offset) {
							case 1:
								b = map.getRowLabel();
								break;
							case 2:
								b = map.getColumnLabel();
								break;
							case 3:
								b = map.getTimeStamp();
								break;
							case 4:
								b = map.getValue();
								break;
							default:
								throw new FieldNumberOutOfBoundException();
						}
                        break;
                    default:
						throw new InvalidTypeException();
                }

                op_res = false;
				switch (comparison_type.attrType) {
					case AttrType.attrInteger:
						comp_res = compare((Integer) a, (Integer) b);
						break;
					case AttrType.attrReal:
						comp_res = compare((Double) a, (Double) b);
						break;
					case AttrType.attrString:
						comp_res = compare((String) a, (String) b);
						break;
					default:
						throw new InvalidTypeException();
				}

                switch (temp_ptr.op.attrOperator) {
                    case AttrOperator.aopEQ:
						if (comp_res == 0) op_res = true;
                        break;
                    case AttrOperator.aopLT:
                        if (comp_res < 0) op_res = true;
                        break;
                    case AttrOperator.aopGT:
                        if (comp_res > 0) op_res = true;
                        break;
                    case AttrOperator.aopNE:
                        if (comp_res != 0) op_res = true;
                        break;
                    case AttrOperator.aopLE:
                        if (comp_res <= 0) op_res = true;
                        break;
                    case AttrOperator.aopGE:
                        if (comp_res >= 0) op_res = true;
                        break;
                    case AttrOperator.aopNOT:
                        if (comp_res != 0) op_res = true;
                        break;
                    default:
                        throw new PredEvalException("Invalid Operation!");
                }

                row_res = row_res || op_res;
                if (row_res == true) {
                    break; // OR predicates satisfied.
				}
				temp_ptr = temp_ptr.next;
			}

			col_res = col_res && row_res;
            if (col_res == false) {
				return false;
			}

            row_res = false; // Starting next row.
        }

        return true;

    }
}