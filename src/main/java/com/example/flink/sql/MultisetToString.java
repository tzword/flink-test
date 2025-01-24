package com.example.flink.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.util.CollectionUtil;

public class MultisetToString extends ScalarFunction {

    public List<String> eval(@DataTypeHint("MULTISET<STRING>") Map<String, Integer> multiset) {
        if (CollectionUtil.isNullOrEmpty(multiset)) {
            return null;
        }
        return new ArrayList<>(multiset.keySet());
    }
}