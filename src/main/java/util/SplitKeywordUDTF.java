package util;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;


/**
 * @Author: PJ, SATAN LOVES YOU FOREVER
 * @Date: 2023/11/10 10:41
 * @Function:
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitKeywordUDTF extends TableFunction<Row> {
    public void eval(String keyword) {
        //use smart: 贪婪模式
        IKSegmenter ikSegmenter = new IKSegmenter(new StringReader(keyword),false);
        try {
            Lexeme lexeme;
            while((lexeme=ikSegmenter.next()) != null) {
                collect(Row.of(lexeme.getLexemeText()));
            }
        } catch (IOException e) {
            collect(Row.of(keyword));
        }

    }
}
