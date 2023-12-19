package de.ddm.actors.profiling;

import java.util.HashSet;

public class Column {
    private int id;
    private HashSet<String> values;
    private String columnName;
    private String nameOfFile;


    public Column(int id, String columnName, String nameOfFile) {
        this.id = id;
        this.values = new HashSet<>();
        this.columnName = columnName;
        this.nameOfFile = nameOfFile;
    }

    public int getId() {
        return id;
    }

    public HashSet<String> getValues() {
        return values;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getNameOfFile() {
        return nameOfFile;
    }
    public void addValueToColumn(String value){
        this.values.add(value);
    }
}
