package simpleOrm;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by jacob on 8/5/2017.
 */

@Retention(RetentionPolicy.RUNTIME)
@interface Column {
    String columnName();
}

@Retention(RetentionPolicy.RUNTIME)
@interface Table {
    String tableName();
}


class PersonRow {

    @Column(columnName = "name")
    String name;

    @Column(columnName = "age")
    int age;
}

@Table(tableName = "person")
class PersonTable {
    Connection connection;
    public PersonTable(Connection connection){
        this.connection = connection;
    }

    String getTableName(){
        return PersonTable.class.getAnnotation(Table.class).tableName();
    }

    public boolean tableExists() throws SQLException {
        Statement st = connection.createStatement();
        ResultSet rs = st.executeQuery(String.format("SELECT to_regclass('public.%s')", getTableName()));
        rs.next();
        String to_regclass = rs.getString("to_regclass");
        if(to_regclass == null)
            return false;
        return true;

    }

    public void createTable() throws SQLException {

        StringBuilder builder = new StringBuilder();
        builder.append(String.format("CREATE TABLE %s (\n", getTableName()));

        Map<Class, String> classMapper = new HashMap<Class, String>();
        classMapper.put(int.class, "INT");
        classMapper.put(Integer.class, "INT");
        classMapper.put(String.class, "VARCHAR");

        List<Field> fields = Arrays.asList(PersonRow.class.getDeclaredFields()).stream().filter((field) ->field.isAnnotationPresent(Column.class)).collect(Collectors.toList());
        for(int i=0; i<fields.size(); i++){
            Field field = fields.get(i);
            if(field.isAnnotationPresent(Column.class)){
                String colName = field.getAnnotation(Column.class).columnName();
                String type = classMapper.get(field.getType());
                builder.append(String.format("%s %s", colName, type));
                if(i < fields.size()-1)
                    builder.append(",");
                builder.append(System.getProperty("line.separator"));
            }
        }

        builder.append(")");
        String command = builder.toString();
        System.out.println(command);
        Statement st = connection.createStatement();
        st.execute(command);
    }

    public void dropTable() throws SQLException{
        Statement st = connection.createStatement();
        st.execute(String.format("DROP TABLE %s", getTableName()));
    }
}

public class Test {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Connection connection = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/postgres", "postgres", "cramer");

        PersonTable personTable = new PersonTable(connection);
        System.out.println("Exists? " + personTable.tableExists());

//        personTable.createTable();
        personTable.dropTable();

//        Statement st = connection.createStatement();
//        ResultSet rs = st.executeQuery("select * from people");
//        while(rs.next()){
//            System.out.println(String.format("%s %s %s", rs.getString("name"), rs.getInt("age"), rs.getString("address")));
//        }

    }
}
