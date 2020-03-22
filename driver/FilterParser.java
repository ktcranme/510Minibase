package driver;

import global.AttrOperator;
import global.AttrType;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class FilterParser {
    public final static int fieldOff[] = {1, 2, 4};
    public final static int fieldTypes[] = {AttrType.attrString, AttrType.attrString, AttrType.attrString};
    public static final String ATTRIBUTE_SEPARATOR = "##";
    //public static final String CONDITION_SEPARATOR = "~";

    // accepts input as row conditions##columnn conditions##value conditions, for select all use *.
    // each condition requires input as symbol(<,>,<=,>=,=,!=) followed by value. For multiple conditions split them with "~".
    // eg. *##[ab,e]##abc : Select all for rows, Column<S and Column>=B, Value = abc.
    public static CondExpr[] parseCombine(String filterString) {
        String fieldFilters[] = filterString.split(ATTRIBUTE_SEPARATOR);
        List<List<CondExpr>> allFilters = new ArrayList<>();
        for (int i = 0; i < fieldFilters.length; i++) {
            if (!fieldFilters[i].equalsIgnoreCase("*")) {
                allFilters.add(fieldFilterGen(fieldFilters[i], fieldOff[i], fieldTypes[i]));
            }
        }
        List<CondExpr> flatAllFilters = allFilters.stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());

        return flatAllFilters.toArray(new CondExpr[flatAllFilters.size()]);
    }

    public static CondExpr[] parseSingle(String filterString, int field_off, int type) {
        List<CondExpr> flatAllFilters = fieldFilterGen(filterString, field_off, type);
        return flatAllFilters.toArray(new CondExpr[flatAllFilters.size()]);
    }

    public static CondExpr[] parseSingleIndex(String filterString, int field_off, int type) {
        List<CondExpr> flatAllFilters = fieldFilterGen(filterString, field_off, type);
        flatAllFilters.add(null);
        return flatAllFilters.toArray(new CondExpr[flatAllFilters.size()]);
    }

    public static List<CondExpr> fieldFilterGen(String s, int off, int type){
        List<CondExpr> fFilters = new ArrayList<>();
        CondExpr temp = null;
        if(!s.contains("[")){
            temp = new CondExpr();
            temp.type1 = new AttrType(AttrType.attrSymbol);
            temp.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), off);
            temp.op = new AttrOperator(AttrOperator.aopEQ);
            temp.type2 = new AttrType(type);
            temp.operand2.string = s;
            temp.next = null;
            fFilters.add(temp);
        } else {
            String vals[] = s.substring(1,s.length()-1).split(",");
            temp = new CondExpr();
            temp.type1 = new AttrType(AttrType.attrSymbol);
            temp.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), off);
            temp.op = new AttrOperator(AttrOperator.aopGE);
            temp.type2 = new AttrType(type);
            temp.operand2.string = vals[0];
            temp.next = null;
            fFilters.add(temp);
            temp = new CondExpr();
            temp.type1 = new AttrType(AttrType.attrSymbol);
            temp.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), off);
            temp.op = new AttrOperator(AttrOperator.aopLE);
            temp.type2 = new AttrType(type);
            temp.operand2.string = vals[1];
            fFilters.add(temp);
        }
        return fFilters;
    }

    /*public static List<CondExpr> fieldFilterGen(String s, int off, int type) {
        String fieldCond[] = s.split(CONDITION_SEPARATOR);
        List<CondExpr> fFilters = new ArrayList<>();
        CondExpr temp = null;
        int indOfValue = 0;
        for (String st : fieldCond) {
            temp = new CondExpr();
            temp.type1 = new AttrType(AttrType.attrSymbol);
            temp.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
            temp.type2 = new AttrType(type);
            if (st.contains("<=")) {
                temp.op = new AttrOperator(AttrOperator.aopLE);
                indOfValue = 2;
            } else if (st.contains("<")) {
                temp.op = new AttrOperator(AttrOperator.aopLT);
                indOfValue = 1;
            } else if (st.contains(">")) {
                temp.op = new AttrOperator(AttrOperator.aopGT);
                indOfValue = 1;
            } else if (st.contains(">=")) {
                temp.op = new AttrOperator(AttrOperator.aopGE);
                indOfValue = 2;
            } else if (st.contains("=")) {
                temp.op = new AttrOperator(AttrOperator.aopEQ);
                indOfValue = 1;
            } else if (st.contains("!=")) {
                temp.op = new AttrOperator(AttrOperator.aopNE);
                indOfValue = 2;
            }
            switch (type) {
                case 0:
                    temp.operand2.string = st.substring(indOfValue);
                    break;
                case 1:
                    temp.operand2.integer = Integer.parseInt(st.substring(indOfValue));
                    break;
                case 2:
                    temp.operand2.real = Float.parseFloat(st.substring(indOfValue));
                    break;
            }
            temp.next = null;
            fFilters.add(temp);
        }
        return fFilters;
    }*/
}
