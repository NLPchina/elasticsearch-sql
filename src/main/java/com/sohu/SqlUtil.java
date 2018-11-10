package com.sohu;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.druid.sql.parser.SQLStatementParser;

import java.util.List;

/**
 * @author zhongshu
 * @since 2018/6/29 下午4:43
 */
public class SqlUtil {

    public static MySqlSelectQueryBlock parseSqlStrToObj(String sql) {
        SQLStatementParser parser = new MySqlStatementParser(sql);// 直接指定使用MySQL Parser，以后遇到其他数据库的sql再扩展吧
        SQLStatement statement = parser.parseStatement(); // 使用Parser解析生成AST，这里SQLStatement statement就是AST
        MySqlSelectQueryBlock sqlSelectQueryBlock = (MySqlSelectQueryBlock) ((SQLSelectStatement) statement).getSelect().getQuery();
        return sqlSelectQueryBlock;
    }

    public static int getGroupByFieldCount(String sql) {
        try {
            MySqlSelectQueryBlock sqlSelectQueryBlock = parseSqlStrToObj(sql);

            SQLSelectGroupByClause groupByClause = sqlSelectQueryBlock.getGroupBy();
            if (groupByClause != null){
                List<SQLExpr> list = groupByClause.getItems();
                return list.size();
            } else {
                return 0;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
    }

    public static void main(String[] args) {
        String sql = "select a,b,c from tbl group by a,b,c";
        System.out.println(getGroupByFieldCount(sql));;
    }
}
