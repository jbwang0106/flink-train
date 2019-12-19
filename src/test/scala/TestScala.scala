import com.jbwang.flink.project.MySqlSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestScala {

    def main(args: Array[String]): Unit = {

        import org.apache.flink.api.scala._

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.addSource(new MySqlSource)
            .setParallelism(1)
            .print()
            .setParallelism(1)

        env.execute("test scala")

    }

}
